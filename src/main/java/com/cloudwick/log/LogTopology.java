package com.cloudwick.log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.cassandra.bolt.AckStrategy;
import backtype.storm.contrib.cassandra.bolt.CassandraBolt;
import backtype.storm.contrib.cassandra.bolt.CassandraCounterBatchingBolt;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

public class LogTopology {

  public static void main(String[] args) throws Exception {

    Config config = new Config();

    //specify kafka hosts
    Conf.KAFKA_HOSTS.add(new HostPort("localhost", 9092));
    Conf.ZOOKEEPER_SERVERS.add("localhost");

    //build kafka spout object
    SpoutConfig kafkaConf = new SpoutConfig(
            new KafkaConfig.StaticHosts(Conf.KAFKA_HOSTS, Conf.PARTITIONS),
            Conf.KAFKA_TOPIC,
            Conf.KAFKA_ZOOKEEPER_PATH,
            Conf.KAFKA_CONSUMER_ID);
//        kafkaConf.zkServers = Conf.ZOOKEEPER_SERVERS;
//        kafkaConf.zkPort = Conf.ZOOKEEPER_PORT;

    KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);

    kafkaConf.scheme = new StringScheme();

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", kafkaSpout, 2);

    builder.setBolt("parser", new ParseBolt(), 2).shuffleGrouping("spout");

    builder.setBolt("volumeCounterOneMin", new VolumeCountBolt(), 2).shuffleGrouping("parser");

    builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("volumeCounterOneMin");

    //persist data to cassandra
    config.put(CassandraBolt.CASSANDRA_HOST, Conf.CASSANDRA_HOST);
    config.put(CassandraBolt.CASSANDRA_KEYSPACE, Conf.CASSANDRA_KEYSPACE);

    //create a CassandraBolt that writes to the "CASSANDRA_COUNT_CF_NAME" column family and uses the Tuple field
    // "FIELD_ROW_KEY" as the row key and "FIELD_INCREMENT" as the increment value
    CassandraCounterBatchingBolt logPersistenceBolt = new CassandraCounterBatchingBolt(
            Conf.CASSANDRA_COUNT_CF_NAME,
            VolumeCountBolt.FIELD_ROW_KEY,
            VolumeCountBolt.FIELD_INCREMENT);
    logPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);

    builder.setBolt("countPersistor", logPersistenceBolt, 5).shuffleGrouping("volumeCounterOneMin");

    config.setDebug(true);

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
