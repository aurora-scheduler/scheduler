package com.paypal.aurora.scheduler.offers;

public class Resource {
  public double cpu;
  public double memory;
  public double disk;
  public Resource(double cpu, double memory, double disk){
    this.cpu = cpu;
    this.memory = memory;
    this.disk = disk;
  }
}
