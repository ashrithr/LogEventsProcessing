Description:
===========

Log Processing storm topology to count the log events per minute and status codes from http logs and parallely persist the events, this is a example storm-topology, illustrating integration between storm, kafka, logstash and cassandra

Dependencies:
============

This storm topology depends on various components

1. `logstash`, will aggregate logs on individual machines and ships them off to `kafka`
2. `kafka`, will act as a message queue from which storm will fetch the log events
3. `cassandra`, will be used by storm to persist the event counters like `logEventsPerMinute` and `logResponseCodes`

Configuring Dependencies:
========================

##LogStash:

logstash does not has support for kafka input/output types yet, so built one using any of the following 2 steps:

1. Install logstash with a branch that supports kafka:

  ```
  git clone https://github.com/ashrithr/logstash.git
  cd logstash
  git checkout feature/kafka

  rvm install jruby-1.7.2
  rvm use jruby-1.7.2

  ruby gembag.rb logstash.gemspec #to install ruby dependencies

  make #to create logstash jar
  ```

2. Use the already built jar with kafka support, found in this project root `logstash-1.1.10.dev-monolithic.jar`

###Configure logstash agent

Then configure your logstash to ship the logs, use the below configuration as a base-line

**shipper.conf**

```
input {
  file {
    type => "syslog"
    path => "/tmp/apache.log"
    debug => true
  }
}

filter {
  multiline {
    type => "syslog"
    pattern => "^\t"
    what => "previous"
  }
}

output {
  stdout {
    debug => true
    debug_format => "json"
  }
  kafka {
    host => "127.0.0.1"
    port => 9092
    topic => "logstash"
  }
}
```

How to run in local mode:
========================

1. Start a local instance of kafka & zookeeper, for [installation instructions](http://kafka.apache.org/)

  ```
  ${KAFKA_HOME}/bin/zookeeper-server-start.sh config/zookeeper.properties
  ${KAFKA_HOME}/bin/kafka-server-start.sh config/server.properties
  ```

2. Start a local instance of logstash

  ```
  java -jar logstash-<version>-monolithic.jar agent -f shipper.conf #if using jar
  ${LOGSTASH_HOME}/bin/logstash agent -f shipper.conf #if using source
  ```

3. Mock random apache log generation, following command will generate random http logs to `/tmp/apache.log` with a time interval of `0.1 seconds`

  ```
  ruby random_log_gen.rb --file /tmp/apache.log --quantity 30
  ```

  There is also a scala variant of the random data generator, feel free to use either of them.

  ```
  ./RandomHttpLogGen.scala /tmp/apache.log 30
  ```

  Note: `random_log_gen.rb` can be found in project root dir, also change the values as required to match the logstash agent conf

4. Start a local cassandra instance, for [installation instructions](http://wiki.apache.org/cassandra/GettingStarted)

  ```
  ${CASSANDRA_HOME}/bin/cassandra -f
  ```

5. Create cassandra schema from file `resources/cassandra_schema.txt`:

  ```
  ${CASSANDRA_HOME}/bin/cassandra-cli -host localhost -port 9160 -f resources/cassandra_schema.txt
  ```

6. Finally, run the storm topology in `LocalCluster` mode

  ```
  mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.cloudwick.log.LogTopology
  ```

Version Used:
============
* kafka - 0.7.2
* storm - 0.8.2
* cassandra - 1.0.12
* storm-kafka - 0.8.0-wip4
