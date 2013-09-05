package com.cloudwick.log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class greps out ipAddress, statusCode from http log event
 */
public class LogEventParserBolt extends BaseRichBolt {
  private OutputCollector collector;

  private static ArrayList<String> getStatusCode(String logLine) {
    ArrayList<String> ipStatusCode = new ArrayList<String>();
    String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"$";
    int NUM_FIELDS = 9;
    Pattern p = Pattern.compile(logEntryPattern);
    Matcher matcher = p.matcher(logLine);
    if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
      return null;
    } else {
      ipStatusCode.add(matcher.group(1));
      ipStatusCode.add(matcher.group(6));
      return ipStatusCode;
    }
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    LogEntry entry = (LogEntry) tuple.getValueByField(FieldNames.LOG_ENTRY);
    List<String> ipStatus = getStatusCode(entry.getMessage());
    String ip = ipStatus.get(0);
    String statusCode = ipStatus.get(1);
    collector.emit(new Values(ip, statusCode));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(FieldNames.LOG_IP, FieldNames.LOG_STATUS_CODE));
  }
}
