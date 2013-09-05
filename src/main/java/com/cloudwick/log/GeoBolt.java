package com.cloudwick.log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;

import java.util.Map;

/**
 * This class will fetch geo status of an ip
 */
public class GeoBolt extends BaseRichBolt {
  private IPResolver ipResolver;
  private OutputCollector collector;

  public GeoBolt(IPResolver ipResolver) {
    this.ipResolver = ipResolver;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    String ip = tuple.getStringByField(FieldNames.LOG_IP);
    JSONObject json = ipResolver.resolveIP(ip);
    String city = (String) json.get(FieldNames.CITY);
    String country = (String) json.get(FieldNames.COUNTRY_NAME);
    collector.emit(new Values(country, city));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(FieldNames.COUNTRY,  FieldNames.CITY));
  }
}
