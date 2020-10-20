/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.paypal.aurora.scheduler.offers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluginConfig {
  private static final Logger LOG = LoggerFactory.getLogger(PluginConfig.class);
  private String endpoint = "http://localhost:9090";
  /*
    aurora-plugin.json file:
    {
      "host": "localhost",
      "port": 9090
    }
   */
  public PluginConfig() {
    final String  configFile = "/etc/magicmatch/aurora-plugin.json";
    // load file
    String jsonStr = null;
    try {
      jsonStr = FileUtils.readFileToString(new File(configFile), StandardCharsets.UTF_8);
    } catch (IOException io) {
      LOG.error("Cannot load " + configFile + "\n " + io.toString());
    }
    if (jsonStr == null || "".equals(jsonStr)) {
      LOG.error(configFile + " is empty");
    } else {
      Config config = new Gson().fromJson(jsonStr, Config.class);
      if (config == null) {
        LOG.error(configFile + " is invalid.");
      } else {
        this.endpoint = "http://" + config.host + ":" + config.port;
      }
    }
  }

  public String getEndpoint() {
    return this.endpoint;
  }

  // for parsing json config file.
  static class Config {
    String host;
    int port;
  }
}
