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
package io.github.aurora.scheduler.scheduling;

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
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.CommandLine;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.scheduling.TaskAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * The default TaskAssigner implementation.
 */
public class ProbabilisticPriorityAssignerModule extends AbstractModule {
  private static final Logger LOG =
      LoggerFactory.getLogger(ProbabilisticPriorityAssignerModule.class);

  private final Options options;

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-probabilistic_priority_assigner_exponent")
    Double probabilisticPriorityAssignerExponent = 0.0;

    @Parameter(names = "-probabilistic_priority_assigner_task_fetch_interval")
    TimeAmount probabilisticPriorityAssignerTaskFetchInterval = new TimeAmount(1, Time.SECONDS);
  }

  public ProbabilisticPriorityAssignerModule(CliOptions mOptions) {
    options = mOptions.getCustom(Options.class);
  }

  static {
    // Statically register custom options for CLI parsing.
    CommandLine.registerCustomOptions(new ProbabilisticPriorityAssignerModule.Options());
  }

  @Override
  protected void configure() {
    bind(Double.class)
        .annotatedWith(ProbabilisticPriorityAssigner.Exponent.class)
        .toInstance(options.probabilisticPriorityAssignerExponent);
    bind(TaskAssigner.class).to(ProbabilisticPriorityAssigner.class).in(Singleton.class);
    bind(TaskFetcher.class).in(com.google.inject.Singleton.class);
    bind(ScheduledExecutorService.class)
        .annotatedWith(Executor.class)
        .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor("HttpOfferSet-%d", LOG));
    bind(Integer.class)
        .annotatedWith(TaskFetchInvervalMs.class)
        .toInstance(options.probabilisticPriorityAssignerTaskFetchInterval.
            as(Time.MILLISECONDS).intValue());
    bind(StatUpdater.class).in(com.google.inject.Singleton.class);
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder()).to(StatUpdater.class);
  }

  // to bind an executor object to StatUpdater using @Executor
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface Executor { }

  // to bind probabilisticPriorityAssignerTaskFetchInterval value to StatUpdater
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface TaskFetchInvervalMs { }

  static class StatUpdater extends AbstractIdleService {
    private final ScheduledExecutorService executor;
    private final TaskFetcher taskFetcher;
    private final Integer taskFetchIntervalMs;

    @Inject
    StatUpdater(
        @Executor ScheduledExecutorService mExecutor,
        TaskFetcher mTaskFetcher,
        @TaskFetchInvervalMs Integer mTaskFetchIntervalMs) {
      executor = requireNonNull(mExecutor);
      taskFetcher = requireNonNull(mTaskFetcher);
      taskFetchIntervalMs = mTaskFetchIntervalMs;
    }

    @Override
    protected void startUp() {
      executor.scheduleAtFixedRate(taskFetcher, 0, taskFetchIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() {
      // Ignored.
    }
  }
}
