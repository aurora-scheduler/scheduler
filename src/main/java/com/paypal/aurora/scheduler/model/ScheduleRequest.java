package com.paypal.aurora.scheduler.model;

public class ScheduleRequest {
  public Resource request;
  public Host[]hosts;
  public ScheduleRequest(){
    request = new Resource(0.0,0.0,0.0);
  }
}