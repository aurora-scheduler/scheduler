package com.paypal.aurora.scheduler.offers;

import org.apache.aurora.scheduler.resources.ResourceBag;

public class ScheduleRequest {
  public Resource request;
  public Host []hosts;
  public ScheduleRequest(){
    request = new Resource(0.0,0.0,0.0);
  }
}