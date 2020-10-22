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
package io.github.aurora.scheduler.offers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpPluginConfig {
  private static final Logger LOG = LoggerFactory.getLogger(HttpPluginConfig.class);
  private String endpoint = "http://localhost:9090";
  private Config config;
  private static final int DEFAULT_LOG_STEP = 1000;
  /*
    aurora-plugin.json file:
    {
      "host": "localhost",
      "port": 9090,
      "debug": true,
      "logStepInTaskNum": 100
    }
   */
  public HttpPluginConfig() {
    final String configFile = "/etc/aurora-scheduler/http-endpoint.json";
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
      config = new Gson().fromJson(jsonStr, Config.class);
      if (config == null) {
        LOG.error(configFile + " is invalid.");
      } else {
        this.endpoint = "http://" + config.host + ":" + config.port;
        LOG.info("Aurora-scheduler uses HttpOfferSet for scheduling at "
            + this.endpoint);
      }
    }
  }

  public String getEndpoint() {
    return this.endpoint;
  }

  public boolean isDebug() {
    if (config != null) {
      return this.config.debug;
    }
    return false;
  }

  public int getLogStepInTaskNum() {
    if (config != null) {
      return this.config.logStepInTaskNum;
    }
    return DEFAULT_LOG_STEP;
  }

  // for parsing json config file.
  static class Config {
    String host;
    int port;
    boolean debug = false;
    int logStepInTaskNum = DEFAULT_LOG_STEP;
  }
}
