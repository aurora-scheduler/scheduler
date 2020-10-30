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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpPluginConfig is used to represent the configuration this HttpOfferSetModule.
 * It has the host & port of the external scheduling unit that this plugin communicates with.
 * It loads the configuration from a file. An file example is as follows.
 *  http-plugin.json file:
 *   {
 *     "url": "http://localhost:9090/v1/offerset",
 *     "debug": true,
 *     "logStepInTaskNum": 100
 *   }
 */
public class HttpPluginConfig {
  private static final Logger LOG = LoggerFactory.getLogger(HttpPluginConfig.class);
  private static final int DEFAULT_LOG_STEP = 1000;

  public static final String CONFIG_FILE = "/etc/aurora-scheduler/http-endpoint.json";

  private URL url;
  private Config config;
  private int timeoutMillisec;
  private boolean ready;

  public HttpPluginConfig() throws MalformedURLException {
    // load file
    String jsonStr = null;
    try {
      jsonStr = FileUtils.readFileToString(new File(CONFIG_FILE), StandardCharsets.UTF_8);
    } catch (IOException io) {
      LOG.error("Cannot load " + CONFIG_FILE + "\n " + io.toString());
    }
    if (jsonStr == null || "".equals(jsonStr)) {
      LOG.error(CONFIG_FILE + " is empty");
    } else {
      config = new Gson().fromJson(jsonStr, Config.class);
      if (config == null) {
        LOG.error(CONFIG_FILE + " is invalid.");
      } else {
        this.url = new URL(config.url);
        this.timeoutMillisec = config.timeoutMillisec;
        this.ready = true;
        LOG.info("HttpOfferSetModule url: " + this.url
               + ", timeout (seconds): " + this.timeoutMillisec + "\n");
      }
    }
  }

  public URL getUrl() {
    return this.url;
  }

  public boolean isReady() {
    return this.ready;
  }

  // getTimeout returns timeout in milliseconds.
  public int getTimeout() {
    return this.timeoutMillisec;
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
    String url;
    boolean debug = false;
    int logStepInTaskNum = DEFAULT_LOG_STEP;
    int timeoutMillisec;
  }
}
