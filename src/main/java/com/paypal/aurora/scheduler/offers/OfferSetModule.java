package com.paypal.aurora.scheduler.offers;

import javax.inject.Singleton;

import com.google.common.collect.Ordering;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;

import com.google.inject.TypeLiteral;

import org.apache.aurora.scheduler.config.CliOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferOrderBuilder;
import org.apache.aurora.scheduler.offers.OfferSet;

public class OfferSetModule extends AbstractModule {
  private final CliOptions options;
  private static final Logger LOG = LoggerFactory.getLogger(OfferSetModule.class);

  public OfferSetModule(CliOptions options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    LOG.info("PayPal Offer Set module Enabled.");
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<Ordering<HostOffer>>() { })
            .toInstance(OfferOrderBuilder.create(options.offer.offerOrder));
        bind(OfferSetImpl.class).in(Singleton.class);
        bind(OfferSet.class).to(OfferSetImpl.class);
        expose(OfferSet.class);
      }
    });
  }
}
