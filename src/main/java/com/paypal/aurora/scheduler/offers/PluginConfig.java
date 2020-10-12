package com.paypal.aurora.scheduler.offers;

import com.paypal.aurora.scheduler.model.Config;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class PluginConfig {
  private static final Logger LOG = LoggerFactory.getLogger(PluginConfig.class);
  private String endpoint = "http://localhost:9090";
  private String configFile = "/etc/aurora-plugin.json";
  /*
    aurora-plugin.json file:
    {
      "host": "localhost",
      "port": 9090
    }
   */
  public PluginConfig() {
    // load file
    FileInputStream targetFile = null;
    try {
      targetFile = new FileInputStream(new File(this.configFile));
    } catch (FileNotFoundException e) {
      LOG.error("Cannot load "+this.configFile);
      LOG.error(e.toString());
    }
    String jsonStr = "";
    try {
      jsonStr = IOUtils.toString(targetFile, "UTF-8");
    } catch (IOException io){
      LOG.error("Cannot load "+this.configFile);
      LOG.error(io.toString());
    }
    if (!jsonStr.equals("")){
      Config config = new Gson().fromJson(jsonStr, Config.class);
      if (config!=null) {
        this.endpoint = "http://"+config.host+":"+config.port;
      }
    } else {
      LOG.error(this.configFile+" is empty");
    }
  }
      
  public String getEndpoint() {
    return this.endpoint;
  }
}
