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
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.InstanceKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.offers.OfferManager.LaunchException;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.scheduling.TaskAssigner;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver;
import org.apache.mesos.v1.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.inject.TimedInterceptor.Timed;
import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.LOST;

public class ProbabilisticPriorityAssignerImpl implements TaskAssigner {
  private static final Logger LOG = LoggerFactory.
      getLogger(ProbabilisticPriorityAssignerImpl.class);

  @VisibleForTesting
  static final Optional<String> LAUNCH_FAILED_MSG =
      Optional.of("Unknown exception attempting to schedule task.");
  @VisibleForTesting
  static final String ASSIGNER_LAUNCH_FAILURES = "assigner_launch_failures";

  private final AtomicLong launchFailures;

  private final StateManager stateManager;
  private final MesosTaskFactory taskFactory;
  private final OfferManager offerManager;
  private final UpdateAgentReserver updateAgentReserver;
  private final Storage storage;
  private Double exponent;

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface Exponent { }

  @Inject
  public ProbabilisticPriorityAssignerImpl(
      StateManager stateManager,
      MesosTaskFactory taskFactory,
      OfferManager offerManager,
      UpdateAgentReserver updateAgentReserver,
      StatsProvider statsProvider,
      Storage storage,
      @Exponent Double exponent) {

    this.stateManager = requireNonNull(stateManager);
    this.taskFactory = requireNonNull(taskFactory);
    this.offerManager = requireNonNull(offerManager);
    this.launchFailures = statsProvider.makeCounter(ASSIGNER_LAUNCH_FAILURES);
    this.updateAgentReserver = requireNonNull(updateAgentReserver);
    this.storage = requireNonNull(storage);
    this.exponent = requireNonNull(exponent);
  }

  @VisibleForTesting
  IAssignedTask mapAndAssignResources(Protos.Offer offer, IAssignedTask task) {
    IAssignedTask assigned = task;
    for (ResourceType type : ResourceManager.getTaskResourceTypes(assigned)) {
      if (type.getMapper().isPresent()) {
        assigned = type.getMapper().get().mapAndAssign(offer, assigned);
      }
    }
    return assigned;
  }

  private Protos.TaskInfo assign(
      MutableStoreProvider storeProvider,
      Protos.Offer offer,
      String taskId,
      boolean revocable) {

    String host = offer.getHostname();
    IAssignedTask assigned = stateManager.assignTask(
        storeProvider,
        taskId,
        host,
        offer.getAgentId(),
        task -> mapAndAssignResources(offer, task));
    LOG.info(
        "Offer on agent {} (id {}) is being assigned task for {}.",
        host, offer.getAgentId().getValue(), taskId);
    return taskFactory.createFrom(assigned, offer, revocable);
  }

  private void launchUsingOffer(
      MutableStoreProvider stores,
      ResourceRequest resourceRequest,
      IAssignedTask task,
      HostOffer offer) throws LaunchException {

    String taskId = task.getTaskId();
    Protos.TaskInfo taskInfo =
        assign(stores, offer.getOffer(), taskId, resourceRequest.isRevocable());
    resourceRequest.getJobState().updateAttributeAggregate(offer.getAttributes());
    try {
      offerManager.launchTask(offer.getOffer().getId(), taskInfo);
    } catch (LaunchException e) {
      LOG.warn("Failed to launch task.", e);
      launchFailures.incrementAndGet();

      // The attempt to schedule the task failed, so we need to backpedal on the assignment.
      // It is in the LOST state and a new task will move to PENDING to replace it.
      // Should the state change fail due to storage issues, that's okay.  The task will
      // time out in the ASSIGNED state and be moved to LOST.
      stateManager.changeState(stores, taskId, Optional.of(ASSIGNED), LOST, LAUNCH_FAILED_MSG);
      throw e;
    }
  }

  private static final class ReservationStatus {
    final boolean taskReserving;
    final Optional<HostOffer> offer;

    private ReservationStatus(boolean taskReserving, Optional<HostOffer> offer) {
      this.taskReserving = taskReserving;
      this.offer = requireNonNull(offer);
    }

    static final ReservationStatus NOT_RESERVING = new ReservationStatus(false, Optional.empty());
    static final ReservationStatus NOT_READY = new ReservationStatus(true, Optional.empty());

    static ReservationStatus ready(HostOffer offer) {
      return new ReservationStatus(true, Optional.of(offer));
    }

    boolean isTaskReserving() {
      return taskReserving;
    }

    Optional<HostOffer> getOffer() {
      return offer;
    }
  }

  private ReservationStatus getReservation(IAssignedTask task, ResourceRequest resourceRequest) {

    IInstanceKey key = InstanceKeys.from(task.getTask().getJob(), task.getInstanceId());
    Optional<String> agentId = updateAgentReserver.getAgent(key);
    if (!agentId.isPresent()) {
      return ReservationStatus.NOT_RESERVING;
    }
    Optional<HostOffer> offer = offerManager.getMatching(
        Protos.AgentID.newBuilder().setValue(agentId.get()).build(),
        resourceRequest);
    if (offer.isPresent()) {
      LOG.info("Used update reservation for {} on {}", key, agentId.get());
      updateAgentReserver.release(agentId.get(), key);
      return ReservationStatus.ready(offer.get());
    } else {
      LOG.info(
          "Tried to reuse offer on {} for {}, but was not ready yet.",
          agentId.get(),
          key);
      return ReservationStatus.NOT_READY;
    }
  }

  /**
   * Determine whether or not the offer is reserved for a different task via preemption or
   * update affinity.
   */
  private boolean isAgentReserved(
      HostOffer offer,
      TaskGroupKey groupKey,
      Map<String, TaskGroupKey> preemptionReservations) {

    String agentId = offer.getOffer().getAgentId().getValue();
    boolean reservedForPreemption = Optional.ofNullable(preemptionReservations.get(agentId))
        .map(group -> !group.equals(groupKey))
        .orElse(false);

    return reservedForPreemption || updateAgentReserver.isReserved(agentId);
  }

  private static class SchedulingMatch {
    final IAssignedTask task;
    final HostOffer offer;

    SchedulingMatch(IAssignedTask task, HostOffer offer) {
      this.task = requireNonNull(task);
      this.offer = requireNonNull(offer);
    }
  }

  private Collection<SchedulingMatch> findMatches(
      ResourceRequest resourceRequest,
      TaskGroupKey groupKey,
      Set<IAssignedTask> tasks,
      Map<String, TaskGroupKey> preemptionReservations) {

    // Avoid matching multiple tasks against any offer.
    Map<String, SchedulingMatch> matchesByOffer = Maps.newHashMap();

    tasks.forEach(task -> {
      ReservationStatus reservation = getReservation(task, resourceRequest);
      Optional<HostOffer> chosenOffer;
      if (reservation.isTaskReserving()) {
        // Use the reserved offer, which may not currently exist.
        chosenOffer = reservation.getOffer();
      } else {
        // Get all offers that will satisfy the given ResourceRequest and that are not reserved
        // for updates or preemption.
        Iterable<HostOffer> matchingOffers = Iterables.filter(
            offerManager.getAllMatching(groupKey, resourceRequest),
            o -> !matchesByOffer.containsKey(o.getOffer().getId().getValue())
                && !isAgentReserved(o, groupKey, preemptionReservations));

        chosenOffer = Optional.ofNullable(Iterables.getFirst(matchingOffers, null));
      }

      chosenOffer.ifPresent(hostOffer -> matchesByOffer.put(
          hostOffer.getOffer().getId().getValue(),
          new SchedulingMatch(task, hostOffer)));
    });

    return matchesByOffer.values();
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
      LOG.info("do not schedule {} to prioritize higher priorities {}", groupKey, prioritySet);
      return new HashSet<String>();
    }

    ImmutableSet.Builder<String> assigned = ImmutableSet.builder();

    for (SchedulingMatch match : findMatches(resourceRequest, groupKey, tasks, reservations)) {
      try {
        launchUsingOffer(storeProvider, resourceRequest, match.task, match.offer);
        assigned.add(match.task.getTaskId());
      } catch (LaunchException e) {
        // Any launch exception causes the scheduling round to terminate for this TaskGroup.
        break;
      }
    }

    return assigned.build();
  }

  /**
   * Determine whether or not schedule the group with priority based on the set of priorities
   * of pending tasks.
   * exponent controls the probabilistic outcome. The higher exponent, the less chance that the low
   * priority tasks can be scheduled.
   * If exponent is greater than 1, it is the exponential distribution.
   * If exponent is 1, it is the uniform distribution.
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

