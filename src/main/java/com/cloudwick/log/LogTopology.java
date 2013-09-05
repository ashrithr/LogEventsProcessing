package com.cloudwick.log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.cassandra.bolt.AckStrategy;
import backtype.storm.contrib.cassandra.bolt.CassandraBatchingBolt;
import backtype.storm.contrib.cassandra.bolt.CassandraBolt;
import backtype.storm.contrib.cassandra.bolt.CassandraCounterBatchingBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.*;

/**
 * This class defines the storm topology, which links spouts and bolts
 */

public class LogTopology {

  public static void main(String[] args) throws Exception {

    Config config = new Config();
    config.setDebug(true);

    /*
     * Configure kafka spouts
     */
    Conf.KAFKA_HOSTS.add(new HostPort("localhost", 9092));
    Conf.ZOOKEEPER_SERVERS.add("localhost");
    SpoutConfig kafkaConf = new SpoutConfig(
            new KafkaConfig.StaticHosts(Conf.KAFKA_HOSTS, Conf.PARTITIONS),
            Conf.KAFKA_TOPIC,
            Conf.KAFKA_ZOOKEEPER_PATH,
            Conf.KAFKA_CONSUMER_ID);
    //      kafkaConf.zkServers = Conf.ZOOKEEPER_SERVERS;
    //      kafkaConf.zkPort = Conf.ZOOKEEPER_PORT;
    KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
    kafkaConf.scheme = new StringScheme();

    /*
     * Configure cassandra bolt
     */
    config.put(CassandraBolt.CASSANDRA_HOST, Conf.CASSANDRA_HOST);
    config.put(CassandraBolt.CASSANDRA_KEYSPACE, Conf.CASSANDRA_KEYSPACE);
    // Create a bolt that writes to the "CASSANDRA_COUNT_CF_NAME" column family and uses the Tuple field
    // "FIELD_ROW_KEY" as the row key and "FIELD_INCREMENT" as the increment value
    CassandraCounterBatchingBolt logPersistenceBolt = new CassandraCounterBatchingBolt(
        Conf.CASSANDRA_COUNT_CF_NAME,
        VolumeCountBolt.FIELD_ROW_KEY,
        VolumeCountBolt.FIELD_INCREMENT);
    logPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
    //cassandra batching bolt to persist the status codes
    CassandraBatchingBolt statusPersistenceBolt = new CassandraBatchingBolt(
        Conf.CASSANDRA_STATUS_CF_NAME,
        StatusCountBolt.FIELD_ROW_KEY
    );
    statusPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);

    /*
     * Creates topology builder
     */
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * Configure storm topology
     */
    builder.setSpout("spout", kafkaSpout, 2);
    builder.setBolt("parser", new ParseBolt(), 2).shuffleGrouping("spout");
    builder.setBolt("volumeCounterOneMin", new VolumeCountBolt(), 2).shuffleGrouping("parser");
    builder.setBolt("countPersistor", logPersistenceBolt, 2).shuffleGrouping("volumeCounterOneMin");
    builder.setBolt("statusParser", new StatusParserBolt(), 2).shuffleGrouping("parser");
    builder.setBolt("statusCounter", new StatusCountBolt(), 3).fieldsGrouping("statusParser", new Fields("statusCode"));
    builder.setBolt("statusCountPersistor", statusPersistenceBolt, 3).shuffleGrouping("statusCounter");

    if(args!=null && args.length > 0) {
      // submit to cluster
      config.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], config, builder.createTopology());
    } else {
      // local cluster
      config.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("kafka", config, builder.createTopology());
      Thread.sleep(50000);
      cluster.shutdown();
    }
  }
}
