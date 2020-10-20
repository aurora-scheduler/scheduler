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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.gson.Gson;
import com.google.inject.Inject;

import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferSet;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation for OfferSet.
 */
@VisibleForTesting
public class MagicMatchOfferSetImpl implements OfferSet {

  private final Set<HostOffer> offers;
  private static final Logger LOG = LoggerFactory.getLogger(MagicMatchOfferSetImpl.class);
  private PluginConfig plugin = null;
  private final Gson gson;

  @Inject
  public MagicMatchOfferSetImpl(Ordering<HostOffer> ordering) {
    offers = new ConcurrentSkipListSet<>(ordering);
    gson = new Gson();
  }

  @Override
  public void add(HostOffer offer) {
    offers.add(offer);
  }

  @Override
  public void remove(HostOffer removed) {
    offers.remove(removed);
  }

  @Override
  public int size() {
    // Potential gotcha - since this is a ConcurrentSkipListSet, size() is more
    // expensive.
    // Could track this separately if it turns out to pose problems.
    return offers.size();
  }

  @Override
  public void clear() {
    offers.clear();
  }

  @Override
  public Iterable<HostOffer> values() {
    return offers;
  }

  @Override
  public Iterable<HostOffer> getOrdered(TaskGroupKey groupKey, ResourceRequest resourceRequest) {
    List<HostOffer> orderedOffers = getOffersFromPlugin(resourceRequest);
    if (orderedOffers != null) {
      return orderedOffers;
    }
    // fall back to default scheduler.
    LOG.warn("MagicMatch failed to schedule the task. Falling back on default ordering.");
    return offers;
  }

  // getOffersFromPlugin gets the offers from MagicMatch.
  private List<HostOffer> getOffersFromPlugin(ResourceRequest resourceRequest) {
    if (plugin == null) {
      plugin = new PluginConfig();
    }
    List<HostOffer> orderedOffers = new ArrayList<>();
    Map<String, HostOffer> offerMap = new HashMap<>();
    for (HostOffer offer : offers) {
      offerMap.put(offer.getAttributes().getHost(), offer);
    }
    // send the Rest API request to the scheduler plugin
    URL url;
    try {
      url = new URL(plugin.getEndpoint() + "/v1/offerset");
    } catch (MalformedURLException e) {
      LOG.error(e.toString());
      return null;
    }

    // create json request
    Resource req = new Resource(resourceRequest.getResourceBag().valueOf(ResourceType.CPUS),
                          resourceRequest.getResourceBag().valueOf(ResourceType.RAM_MB),
                          resourceRequest.getResourceBag().valueOf(ResourceType.DISK_MB));
    Host[] hosts = new Host[Iterables.size(offers)];
    int i = 0;
    for (HostOffer offer : offers) {
      hosts[i] = new Host();
      hosts[i].name = offer.getAttributes().getHost();
      double cpu = offer.getResourceBag(true).valueOf(ResourceType.CPUS)
          + offer.getResourceBag(false).valueOf(ResourceType.CPUS);
      double memory = offer.getResourceBag(true).valueOf(ResourceType.RAM_MB)
          + offer.getResourceBag(false).valueOf(ResourceType.RAM_MB);
      double disk = offer.getResourceBag(true).valueOf(ResourceType.DISK_MB)
          + offer.getResourceBag(false).valueOf(ResourceType.DISK_MB);
      hosts[i].offer = new Resource(cpu, memory, disk);
      i++;
    }
    ScheduleRequest scheduleRequest = new ScheduleRequest(req, hosts);
    LOG.debug(scheduleRequest.toString());

    // create connection
    HttpURLConnection con;
    try {
      con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");
      con.setRequestProperty("Content-Type", "application/json; utf-8");
      con.setRequestProperty("Accept", "application/json");
      con.setDoOutput(true);
    } catch (ProtocolException pe) {
      LOG.error("The HTTP protocol was not setup correctly. \n" + pe.toString());
      return null;
    } catch (IOException ioe) {
      LOG.error("Unable to open HTTP connection. \n" + ioe.toString());
      return null;
    }
    String jsonStr = gson.toJson(scheduleRequest);
    LOG.debug("request to plugin: " + jsonStr);
    try (OutputStream os = con.getOutputStream()) {
      byte[] input = jsonStr.getBytes(StandardCharsets.UTF_8);
      os.write(input, 0, input.length);
    } catch (UnsupportedEncodingException uee) {
      LOG.error("ScheduleRequest json is not valid.\n" + uee.toString());
      return null;
    } catch (IOException ioe) {
      LOG.error("Unable to send scheduleRequest to MagicMatch .\n" + ioe.toString());
      return null;
    }

    // read response
    StringBuilder response = new StringBuilder();
    try {
      String responseLine = IOUtils.toString(con.getInputStream(), StandardCharsets.UTF_8);
      response.append(responseLine.trim());
    } catch (UnsupportedEncodingException uee) {
      LOG.error("MagicMatch response is not valid.\n" + uee.toString());
      return null;
    } catch (IOException ioe) {
      LOG.error("Unable to read the response from MagicMatch.\n" + ioe.toString());
      return null;
    }
    ScheduleResponse scheduleResponse = gson.fromJson(response.toString(), ScheduleResponse.class);
    LOG.debug("plugin response: " + response.toString());

    // process the scheduleResult
    if (scheduleResponse.error.equals("") && scheduleResponse.hosts != null) {
      for (String host : scheduleResponse.hosts) {
        HostOffer offer = offerMap.get(host);
        if (offer == null) {
          LOG.error("Cannot find this host " + host + " in " + offerMap.toString());
        } else {
          orderedOffers.add(offer);
        }
      }
      return orderedOffers;
    }
    LOG.error("Unable to get sorted offers from MagicMatch due to " + scheduleResponse.error);
    return null;
  }

  // Host represents for each host offer.
  static class Host {
    String name;
    Resource offer;

    @Override
    public String toString() {
      return "Host{" + "name='" + name + '\'' + ", offer=" + offer + '}';
    }
  }

  // Resource is used between Aurora and MagicMatch.
  static class Resource {
    double cpu;
    double memory;
    double disk;

    Resource(double cpu, double memory, double disk) {
      this.cpu = cpu;
      this.memory = memory;
      this.disk = disk;
    }

    @Override
    public String toString() {
      return "Resource{" + "cpu=" + cpu + ", memory=" + memory + ", disk=" + disk + '}';
    }
  }

  // ScheduleRequest is the request sent to MagicMatch.
  static class ScheduleRequest {
    Resource request;
    Host[] hosts;

    ScheduleRequest(Resource request, Host... hosts) {
      this.request = request;
      this.hosts = hosts;
    }

    @Override
    public String toString() {
      return "ScheduleRequest{" + "request=" + request + ", hosts=" + Arrays.toString(hosts) + '}';
    }
  }

  // ScheduleResponse is the scheduling result responded by MagicMatch
  static class ScheduleResponse {
    String error;
    String[] hosts;

    @Override
    public String toString() {
      return "ScheduleResponse{" + "error='" + error + '\'' + ", hosts="
          + Arrays.toString(hosts) + '}';
    }
  }
}
