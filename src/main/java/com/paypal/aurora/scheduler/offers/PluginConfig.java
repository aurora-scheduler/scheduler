package com.paypal.aurora.scheduler.offers;

public class PluginConfig {
  private String endpoint = "http://localhost:9090";
  private String configJson = "/etc/aurora-plugin.json";

  public PluginConfig() {
  }
      
  public PluginConfig(String endpoint, String configJson) {
    this.endpoint = endpoint;
    this.configJson = configJson;
    //TODO: (nhatle) load the aurora-plugin.json
  }
  
  public String getEndpoint() {
    return this.endpoint;
  }
}
