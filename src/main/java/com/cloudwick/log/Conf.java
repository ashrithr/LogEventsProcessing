package com.cloudwick.log;

import storm.kafka.HostPort;

import java.util.ArrayList;
import java.util.List;

public class Conf {
  /*
   * Kafka config
   */
  // list of Kafka brokers
  public static List<HostPort> KAFKA_HOSTS = new ArrayList<HostPort>();
  // partitions per kafka broker
  public static final int PARTITIONS = 1;
  // topic to read from
  public static final String KAFKA_TOPIC = "logstash";
  // root path in zookeeper for the spout to store consumer offsets
  public static final String KAFKA_ZOOKEEPER_PATH = "/kafkastorm";
  // an id of this consumer for storing the consumer offsets in zookeeper
  public static final String KAFKA_CONSUMER_ID = "kafkastormconsumer";

  /*
   * Zookeepers config
   */
  // list of zookeeper servers
  public static List<String> ZOOKEEPER_SERVERS = new ArrayList<String>();
  // port on which zookeepers are listening for incoming connections
  public static final int ZOOKEEPER_PORT = 2181;

  /*
   * Cassandra config
   */
  // cassandra host:port combination, this varies from version of cassandra bolt
  public static final String CASSANDRA_HOST = "localhost:9160";
  // cassandra keyspace to use
  public static final String CASSANDRA_KEYSPACE = "Logging";
  // cassandra column family to use for storing log volume by minute
  public static final String CASSANDRA_COUNT_CF_NAME = "LogVolumeByMinute";
  // cassandra column family to use for storing http status code counts
  public static final String CASSANDRA_STATUS_CF_NAME = "StautusCodesCount";
}