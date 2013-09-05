package com.cloudwick.log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This class will keep the stats of Country, City from which the http req came in
 */

public class GeoStatsBolt extends BaseRichBolt{

  private class CountryStats {
    private int countryTotal = 0;
    private static final int COUNT_INDEX = 0;
    private static final int PERCENTAGE_INDEX = 0;
    private String countryName;
    private Map<String, List<Integer>> cityStats = new HashMap<String, List<Integer>>();

    public CountryStats(String countryName) {
      this.countryName = countryName;
    }

    public void cityFound(String cityName){
      countryTotal++;
      if(cityStats.containsKey(cityName)){
        cityStats.get(cityName).set(COUNT_INDEX, cityStats.get(cityName).get(COUNT_INDEX).intValue() + 1);
      } else {
        List<Integer> list = new LinkedList<Integer>();
        list.add(1);
        list.add(0);
        cityStats.put(cityName, list);
      }

      double percent = (double)cityStats.get(cityName).get(COUNT_INDEX)/(double)countryTotal;
      cityStats.get(cityName).set(PERCENTAGE_INDEX, (int)percent);
    }

    public int getCountryTotal(){return countryTotal;}

    public int getCityTotal(String cityName){
      return cityStats.get(cityName).get(COUNT_INDEX).intValue();
    }

    public String toString(){
      return "Total Count for " + countryName + " is " + Integer.toString(countryTotal) + "\n"
          +  "Cities:  " + cityStats.toString();
    }

  }

  private OutputCollector outputCollector;
  private Map<String, CountryStats> stats = new HashMap<String, CountryStats>();

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    String country = tuple.getStringByField(FieldNames.COUNTRY);
    String city = tuple.getStringByField(FieldNames.CITY);
    if(!stats.containsKey(country)){
      stats.put(country, new CountryStats(country));
    }
    stats.get(country).cityFound(city);
    outputCollector.emit(new Values(country,
        stats.get(country).getCountryTotal(),
        city,
        stats.get(country).getCityTotal(city)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(FieldNames.COUNTRY,
        FieldNames.COUNTRY_TOTAL,
        FieldNames.CITY,
        FieldNames.CITY_TOTAL));
  }
}
