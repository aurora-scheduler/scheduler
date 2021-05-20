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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.CommandLine;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferOrderBuilder;
import org.apache.aurora.scheduler.offers.OfferSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class HttpOfferSetModule extends AbstractModule {
  private final CliOptions cliOptions;
  private final Options options;
  private static final Logger LOG = LoggerFactory.getLogger(HttpOfferSetModule.class);
  static List<Long> latencyMsList = Collections.synchronizedList(new LinkedList<>());
  static List<Long> offerSetDiffList = Collections.synchronizedList(new LinkedList<>());
  private static long failureCount = 0;
  private static boolean enabled = false;

  public static synchronized void incFailureCount() {
    HttpOfferSetModule.failureCount++;
  }

  public static synchronized long getFailureCount() {
    return HttpOfferSetModule.failureCount;
  }

  public static synchronized void resetFailureCount() {
    HttpOfferSetModule.failureCount = 0;
  }

  public static synchronized void enable(boolean mEnabled) {
    HttpOfferSetModule.enabled = mEnabled;
  }

  public static synchronized boolean isEnabled() {
    return HttpOfferSetModule.enabled;
  }

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-http_offer_set_endpoint")
    String httpOfferSetEndpoint = "http://localhost:9090/v1/offerset";

    @Parameter(names = "-http_offer_set_timeout_ms")
    int httpOfferSetTimeoutMs = 100;

    @Parameter(names = "-http_offer_set_max_retries")
    int httpOfferSetMaxRetries = 10;
  }

  static {
    // Statically register custom options for CLI parsing.
    CommandLine.registerCustomOptions(new Options());
  }

  public HttpOfferSetModule(CliOptions mOptions) {
    cliOptions = mOptions;
    options = mOptions.getCustom(Options.class);
  }

  @Override
  protected void configure() {

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<Ordering<HostOffer>>() {
          }).toInstance(OfferOrderBuilder.create(cliOptions.offer.offerOrder));
        bind(Integer.class)
                .annotatedWith(HttpOfferSetImpl.TimeoutMs.class)
                .toInstance(options.httpOfferSetTimeoutMs);
        bind(String.class)
                .annotatedWith(HttpOfferSetImpl.Endpoint.class)
                .toInstance(options.httpOfferSetEndpoint);
        bind(Integer.class)
                .annotatedWith(HttpOfferSetImpl.MaxRetries.class)
                .toInstance(options.httpOfferSetMaxRetries);
        bind(OfferSet.class).to(HttpOfferSetImpl.class).in(Singleton.class);
        expose(OfferSet.class);
      }
    });

    bind(StatCalculator.class).in(com.google.inject.Singleton.class);
    bind(ScheduledExecutorService.class)
            .annotatedWith(Executor.class)
            .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor("HttpOfferSet-%d", LOG));
    bind(Integer.class)
            .annotatedWith(RefreshRateMs.class)
            .toInstance(cliOptions.sla.slaRefreshInterval.as(Time.MILLISECONDS).intValue());
    bind(StatUpdater.class).in(com.google.inject.Singleton.class);
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder()).to(StatUpdater.class);
  }

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface Executor { }

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface RefreshRateMs { }

  static class StatUpdater extends AbstractIdleService {
    private final ScheduledExecutorService executor;
    private final StatCalculator calculator;
    private final Integer refreshRateMs;

    @Inject
    StatUpdater(
            @Executor ScheduledExecutorService mExecutor,
            StatCalculator mCalculator,
            @RefreshRateMs Integer mRefreshRateMs) {
      executor = requireNonNull(mExecutor);
      calculator = requireNonNull(mCalculator);
      refreshRateMs = mRefreshRateMs;
    }

    @Override
    protected void startUp() {
      executor.scheduleAtFixedRate(calculator, 0, refreshRateMs, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() {
      // Ignored.
    }
  }
}
