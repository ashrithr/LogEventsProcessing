package com.cloudwick.log;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

/**
 * Resolves IP to Address (JSON)
 */
public class IPResolver implements Serializable {
  static String url = "http://api.hostip.info/get_json.php";

  public JSONObject resolveIP(String ip) {
    URL geoUrl = null;
    BufferedReader bufferedReader = null;
    try {
      geoUrl = new URL(url + "?ip=" + ip);
      URLConnection connection = geoUrl.openConnection();
      bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      JSONObject json = (JSONObject) JSONValue.parse(bufferedReader);
      bufferedReader.close();
      return json;
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if(bufferedReader != null) {
        try {
          bufferedReader.close();
        } catch (IOException e) {}
      }
    }
    return null;
  }
}
