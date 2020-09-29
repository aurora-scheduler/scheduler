package com.paypal.aurora.scheduler.offers;

import java.sql.Timestamp;
import org.apache.aurora.scheduler.offers.HostOffer;

public class PreviousOffer {
  private Timestamp timestamp;
  private HostOffer offer;

  public PreviousOffer(Timestamp now, HostOffer offer){
    this.timestamp = now;
    this.offer = offer;
  }

  public Timestamp getTimestamp() {
    return this.timestamp;
  }

  public HostOffer getOffer(){
    return this.offer;
  }
}
