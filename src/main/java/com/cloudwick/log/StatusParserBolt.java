package com.cloudwick.log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author: ashrith
 * Date: 8/30/13
 * Time: 12:09 PM
 * Desc:
 */
public class StatusParserBolt extends BaseRichBolt {
  private OutputCollector collector;

  /*
   * returns the status code of the apache log event
   */
  private static String getStatusCode(String logLine) {
    String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"$";
    int NUM_FIELDS = 9;
    Pattern p = Pattern.compile(logEntryPattern);
    Matcher matcher = p.matcher(logLine);
    if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
      return null;
    } else {
      return matcher.group(6);
    }
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    LogEntry entry = (LogEntry) tuple.getValueByField(FieldNames.LOG_ENTRY);
    int statusCode = Integer.parseInt(getStatusCode(entry.getMessage()));
    collector.emit(new Values(statusCode));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("statusCode"));
  }
}
