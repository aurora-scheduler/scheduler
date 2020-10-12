package com.paypal.aurora.scheduler.offers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.google.gson.Gson;

import com.paypal.aurora.scheduler.model.Host;
import com.paypal.aurora.scheduler.model.Resource;
import com.paypal.aurora.scheduler.model.ScheduleRequest;
import com.paypal.aurora.scheduler.model.ScheduleResponse;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferSet;

import org.apache.aurora.scheduler.resources.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation for OfferSet. Backed by a ConcurrentSkipListSet.
 */
@VisibleForTesting
public class OfferSetImpl implements OfferSet {

  private final Set<HostOffer> offers;
  private static final Logger LOG = LoggerFactory.getLogger(OfferSetImpl.class);

  @Inject
  public OfferSetImpl(Ordering<HostOffer> ordering) {
    offers = new ConcurrentSkipListSet<>(ordering);
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

  private Timestamp prevTimestamp =  new Timestamp(System.currentTimeMillis());
  private long ageInMilliseconds = 60*1000;
  @Override
  public Iterable<HostOffer> getOrdered(TaskGroupKey groupKey, ResourceRequest resourceRequest) {
    List<HostOffer> orderedOffers = getOffersFromPlugin(resourceRequest, offers);
    if (orderedOffers != null){
      return orderedOffers;
    }
    // fallback to default scheduler.
    LOG.error("MagicMatch failed to schedule the task. Aurora uses the default scheduler.");
    return offers;
  }
  PluginConfig plugin = null;
  // getOffersFromPlugin gets the offers from MagicMatch.
  private List<HostOffer> getOffersFromPlugin(ResourceRequest resourceRequest, Iterable<HostOffer> offers){
    if (plugin==null){
      plugin = new PluginConfig();
    }
    Gson gson = new Gson();
	  List<HostOffer> orderedOffers = new ArrayList<HostOffer>();
	  Map<String,HostOffer> offerMap = new HashMap<String, HostOffer>();
	  for(HostOffer offer: offers) {
	    offerMap.put(offer.getAttributes().getHost(), offer);
	  }
    // send the Rest API request to the scheduler plugin
	  URL url = null;
    try {
      url = new URL(plugin.getEndpoint() + "/v1/offerset");
    } catch (MalformedURLException e) {
      LOG.error(e.toString());
      return null;
    }
	  // create json request
	  ScheduleRequest scheduleRequest = new ScheduleRequest();
    scheduleRequest.request.cpu = resourceRequest.getResourceBag().valueOf(ResourceType.CPUS);
    scheduleRequest.request.memory = resourceRequest.getResourceBag().valueOf(ResourceType.RAM_MB);
    scheduleRequest.request.disk = resourceRequest.getResourceBag().valueOf(ResourceType.DISK_MB);
    scheduleRequest.hosts = new Host[Iterables.size(offers)];
    int i =0;
	  for (HostOffer offer:offers) {
      scheduleRequest.hosts[i] = new Host();
      scheduleRequest.hosts[i].name = offer.getAttributes().getHost();
      double cpu = offer.getResourceBag(true).valueOf(ResourceType.CPUS) +
          offer.getResourceBag(false).valueOf(ResourceType.CPUS);
      double memory = offer.getResourceBag(true).valueOf(ResourceType.RAM_MB) +
          offer.getResourceBag(false).valueOf(ResourceType.RAM_MB) ;
      double disk = offer.getResourceBag(true).valueOf(ResourceType.DISK_MB) +
          offer.getResourceBag(false).valueOf(ResourceType.DISK_MB);
      scheduleRequest.hosts[i].offer = new Resource(cpu, memory, disk);
      i++;
	  }

    // create connection
    HttpURLConnection con = null;
    try {
      con = (HttpURLConnection)url.openConnection();
      con.setRequestMethod("POST");
      con.setRequestProperty("Content-Type", "application/json; utf-8");
      con.setRequestProperty("Accept", "application/json");
      con.setDoOutput(true);
    } catch (ProtocolException pe) {
      LOG.error(pe.toString());
      return null;
    } catch (IOException ioe) {
      LOG.error(ioe.toString());
      return null;
    }
    String jsonStr = gson.toJson(scheduleRequest);
	  LOG.debug("request to plugin: "+jsonStr);
	  try(OutputStream os = con.getOutputStream()) {
	    byte[] input = jsonStr.getBytes("utf-8");
	    os.write(input, 0, input.length);
	  } catch (UnsupportedEncodingException uee) {
	    LOG.error(uee.toString());
      return null;
    } catch (IOException ioe){
      LOG.error(ioe.toString());
      return null;
    }
	  // read response
	  StringBuilder response = new StringBuilder();
	  try{
      BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
      String responseLine = null;
      while ((responseLine = br.readLine()) != null) {
          response.append(responseLine.trim());
      }
    } catch (UnsupportedEncodingException uee){
      LOG.error(uee.toString());
      return null;
    } catch (IOException ioe) {
      LOG.error(ioe.toString());
      return null;
    }
    ScheduleResponse scheduleResponse = gson.fromJson(response.toString(), ScheduleResponse.class);
    LOG.debug("plugin response: "+response.toString());
    // process the scheduleResult
    if (scheduleResponse.hosts!=null){
      for(String host: scheduleResponse.hosts) {
        HostOffer offer = offerMap.get(host);
        if (offer!=null) {
          orderedOffers.add(offer);
        } else {
          LOG.error("Cannot find this host "+host+" in "+offerMap.size()+" offers");
        }
      }
    }
    return orderedOffers;
  }
}