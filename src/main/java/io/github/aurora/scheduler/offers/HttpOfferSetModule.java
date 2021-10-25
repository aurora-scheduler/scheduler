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
import org.apache.aurora.scheduler.config.types.TimeAmount;
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

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-http_offer_set_endpoint",
        description = "http_offer_set endpoint")
    String httpOfferSetEndpoint = "http://localhost:9090/v1/offerset";

    @Parameter(names = "-http_offer_set_timeout",
        description = "http_offer_set timeout")
    TimeAmount httpOfferSetTimeout = new TimeAmount(100, Time.MILLISECONDS);

    @Parameter(names = "-http_offer_set_max_retries",
        description = "Maximum number of tries to reach the http_offer_set_endpoint")
    int httpOfferSetMaxRetries = 10;

    // the slaves have more than or equal to the httpOfferSetMaxStartingTasksPerSlave
    // are put in the bottom of the offerset. If you want to disable this feature, set
    // httpOfferSetMaxStartingTasksPerSlave less than or equal to zero
    @Parameter(names = "-http_offer_set_max_starting_tasks_per_slave",
        description = "Maximum number of starting tasks per slave are allowed")
    int httpOfferSetMaxStartingTasksPerSlave = 0;

    @Parameter(names = "-http_offer_set_filter_enabled",
        description = "Allow to filter out the bad offers",
        arity = 1)
    boolean httpOfferSetFilterEnabled = false;

    @Parameter(names = "-http_offer_set_task_fetch_interval",
        description = "Interval of fetching starting tasks from task_store")
    TimeAmount httpOfferSetTaskFetchInterval = new TimeAmount(1, Time.SECONDS);
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
            .toInstance(options.httpOfferSetTimeout.getValue().intValue());
        bind(String.class)
            .annotatedWith(HttpOfferSetImpl.Endpoint.class)
            .toInstance(options.httpOfferSetEndpoint);
        bind(Integer.class)
            .annotatedWith(HttpOfferSetImpl.MaxRetries.class)
            .toInstance(options.httpOfferSetMaxRetries);
        bind(Integer.class)
            .annotatedWith(HttpOfferSetImpl.MaxStartingTaskPerSlave.class)
            .toInstance(options.httpOfferSetMaxStartingTasksPerSlave);
        bind(Boolean.class)
            .annotatedWith(HttpOfferSetImpl.FilterEnabled.class)
            .toInstance(options.httpOfferSetFilterEnabled);
        bind(OfferSet.class).to(HttpOfferSetImpl.class).in(Singleton.class);
        expose(OfferSet.class);
      }
    });

    bind(StatCalculator.class).in(com.google.inject.Singleton.class);
    bind(TaskFetcher.class).in(com.google.inject.Singleton.class);

    bind(ScheduledExecutorService.class)
        .annotatedWith(Executor.class)
        .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor("HttpOfferSet-%d", LOG));
    bind(Long.class)
        .annotatedWith(RefreshRateMs.class)
        .toInstance(cliOptions.sla.slaRefreshInterval.as(Time.MILLISECONDS));
    bind(Long.class)
        .annotatedWith(TaskFetcherRateSec.class)
        .toInstance(options.httpOfferSetTaskFetchInterval.as(Time.MILLISECONDS));
    bind(Integer.class)
        .annotatedWith(MaxStartingTaskPerSlave.class)
        .toInstance(options.httpOfferSetMaxStartingTasksPerSlave);
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

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface TaskFetcherRateSec { }

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface MaxStartingTaskPerSlave { }

  static class StatUpdater extends AbstractIdleService {
    private final ScheduledExecutorService executor;
    private final StatCalculator calculator;
    private final TaskFetcher taskFetcher;
    private final Long refreshRateMs;
    private final Long taskFetcherRateMs;
    private final Integer maxStartingTasksPerSlave;

    @Inject
    StatUpdater(
            @Executor ScheduledExecutorService mExecutor,
            StatCalculator mCalculator,
            TaskFetcher mTaskFetcher,
            @RefreshRateMs Long mRefreshRateMs,
            @TaskFetcherRateSec Long mTaskFetcherRateMs,
            @MaxStartingTaskPerSlave Integer mMaxStartingTasksPerSlave) {
      executor = requireNonNull(mExecutor);
      calculator = requireNonNull(mCalculator);
      taskFetcher = requireNonNull(mTaskFetcher);
      refreshRateMs = requireNonNull(mRefreshRateMs);
      taskFetcherRateMs = requireNonNull(mTaskFetcherRateMs);
      maxStartingTasksPerSlave = requireNonNull(mMaxStartingTasksPerSlave);
    }

    @Override
    protected void startUp() {
      executor.scheduleAtFixedRate(calculator, 0, refreshRateMs, TimeUnit.MILLISECONDS);
      if (maxStartingTasksPerSlave > 0) {
        executor.scheduleAtFixedRate(taskFetcher, 0, taskFetcherRateMs, TimeUnit.MILLISECONDS);
      }
    }

    @Override
    protected void shutDown() {
      // Ignored.
    }
  }
}
