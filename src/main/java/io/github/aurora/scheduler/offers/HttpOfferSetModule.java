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

import javax.inject.Singleton;

import com.google.common.collect.Ordering;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferOrderBuilder;
import org.apache.aurora.scheduler.offers.OfferSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpOfferSetModule extends AbstractModule {
  private final CliOptions options;
  private static final Logger LOG = LoggerFactory.getLogger(HttpOfferSetModule.class);

  public HttpOfferSetModule(CliOptions options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    LOG.info("HttpOfferSetModule Enabled.");
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<Ordering<HostOffer>>() {
          }).toInstance(OfferOrderBuilder.create(options.offer.offerOrder));
        bind(OfferSet.class).to(HttpOfferSetImpl.class).in(Singleton.class);
        expose(OfferSet.class);
      }
    });
  }
}
