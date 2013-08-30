package com.cloudwick.log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This bolt will count the status codes from http logs such as 200, 404, 503
 */
public class StatusCountBolt  extends BaseRichBolt {
  public static Logger LOG = Logger.getLogger(VolumeCountBolt.class);
  private OutputCollector collector;

  Map<Integer, Integer> counts;

  public static final String FIELD_ROW_KEY = "word";
  public static final String FIELD_COLUMN = "count";


  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.counts = new HashMap<Integer, Integer>();
    this.collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    int statusCode = (Integer) tuple.getValues().get(0);
    int count = 0;
    if (this.counts.containsKey(statusCode)) {
      count = this.counts.get(statusCode);
    }
    count ++;
    this.counts.put(statusCode, count);
    collector.emit(new Values(statusCode, count));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(FIELD_ROW_KEY, FIELD_COLUMN));
  }
}
