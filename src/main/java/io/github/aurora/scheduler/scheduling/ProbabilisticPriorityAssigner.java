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
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.scheduling.TaskAssignerImpl;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.inject.TimedInterceptor.Timed;

public class ProbabilisticPriorityAssigner extends TaskAssignerImpl {
  private static final Logger LOG = LoggerFactory.
      getLogger(ProbabilisticPriorityAssigner.class);

  private final Storage storage;
  private Double exponent;

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface Exponent { }

  @Inject
  public ProbabilisticPriorityAssigner(
      StateManager stateManager,
      MesosTaskFactory taskFactory,
      OfferManager offerManager,
      UpdateAgentReserver updateAgentReserver,
      StatsProvider statsProvider,
      Storage storage,
      @Exponent Double exponent) {
    super(stateManager, taskFactory, offerManager, updateAgentReserver, statsProvider);
    this.storage = requireNonNull(storage);
    this.exponent = requireNonNull(exponent);
  }

  @Timed("assigner_maybe_assign")
  @Override
  public Set<String> maybeAssign(
      MutableStoreProvider storeProvider,
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Set<IAssignedTask> tasks,
      Map<String, TaskGroupKey> reservations) {

    // probabilistic priority queueing: may not schedule these tasks if
    // there are pending tasks with higher priority.
    Iterable<IScheduledTask> pendindTasks = getPendingTasks();
    Set<Integer> prioritySet = new HashSet<>();
    for (IScheduledTask t: pendindTasks) {
      prioritySet.add(t.getAssignedTask().getTask().getPriority());
    }
    //TODO(lenhattan86): Is the group is always included in the pending task set?
    prioritySet.add(groupKey.getTask().getPriority());

    if (!isScheduled(prioritySet, groupKey.getTask().getPriority())) {
      LOG.info("{} is being skipped to prioritize tasks with a higher priority {}", groupKey, prioritySet);
      return new HashSet<String>();
    }

    return super.maybeAssign(storeProvider, resourceRequest, groupKey, tasks, reservations);
  }

  /**
   * Determine whether or not schedule the group with priority based on the set of priorities
   * of pending tasks.
   * The exponent controls the probabilistic outcome. The higher exponent, the less chance that the low
   * priority tasks can be scheduled.
   * If exponent is greater than 1, it is an exponential distribution.
   * If exponent is 1, it is a uniform distribution.
   * If exponent is 0, there is no probabilistic priority queueing.
   */
  @VisibleForTesting
  boolean isScheduled(Set<Integer> prioritySet, int priority) {
    int maxPriority = 0;
    for (int p: prioritySet) {
      if (maxPriority < p) {
        maxPriority = p;
      }
    }
    double maxExp = Math.pow(maxPriority + 1, exponent);
    double threshold = Math.pow(priority + 1, exponent);

    Random rand = new Random();
    double chance = rand.nextDouble() * maxExp;

    return chance <= threshold;
  }

  @VisibleForTesting
  Iterable<IScheduledTask> getPendingTasks() {
    return Storage.Util.fetchTasks(storage, Query.unscoped().byStatus(ScheduleStatus.PENDING));
  }

  @VisibleForTesting
  void setExponent(Double exp) {
    this.exponent = exp;
  }
}

