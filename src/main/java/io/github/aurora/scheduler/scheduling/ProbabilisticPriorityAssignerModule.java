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

import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.AbstractModule;

import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.config.CommandLine;
import org.apache.aurora.scheduler.scheduling.TaskAssigner;

/**
 * The default TaskAssigner implementation.
 */
public class ProbabilisticPriorityAssignerModule extends AbstractModule {
  private final Options options;

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-probabilistic_priority_assigner_exponent")
    Double probabilisticPriorityAssignerExponent = 0.0;
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
  }
}
