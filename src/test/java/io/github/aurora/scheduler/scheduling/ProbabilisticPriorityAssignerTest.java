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

import java.util.HashSet;
import java.util.Set;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.aurora.scheduler.updater.UpdateAgentReserver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProbabilisticPriorityAssignerTest extends EasyMockTest {

  private ProbabilisticPriorityAssigner assigner;

  @Before
  public void setUp() {
    assigner = new ProbabilisticPriorityAssigner(
        createMock(StateManager.class),
        createMock(MesosTaskFactory.class),
        createMock(OfferManager.class),
        createMock(UpdateAgentReserver.class),
        new FakeStatsProvider(),
        0.0);
  }

  @Test
  public void testIsScheduled() {
    control.replay();

    int numOfTests = 1000;
    Set<Integer> set = new HashSet<>();
    set.add(0);
    set.add(1);
    set.add(2);

    // group with the highest priority is always scheduled
    assigner.setExponent(1.0);
    boolean res = true;
    for (int i = 0; i < numOfTests; i++) {
      res = res & assigner.isScheduled(set, 2);
    }
    assertTrue(res);

    assigner.setExponent(99.0);
    res = true;
    for (int i = 0; i < numOfTests; i++) {
      res = res & assigner.isScheduled(set, 2);
    }
    assertTrue(res);

    // other groups do have chance to be scheduled
    assigner.setExponent(1.0);
    res = false;
    for (int i = 0; i < numOfTests; i++) {
      res = res | assigner.isScheduled(set, 1);
    }
    assertTrue(res);
    res = false;
    for (int i = 0; i < numOfTests; i++) {
      res = res | assigner.isScheduled(set, 0);
    }
    assertTrue(res);

    // groups with the low priority are not always scheduled
    res = true;
    for (int i = 0; i < numOfTests; i++) {
      res = res & assigner.isScheduled(set, 1);
    }
    assertFalse(res);
    res = true;
    for (int i = 0; i < numOfTests; i++) {
      res = res & assigner.isScheduled(set, 0);
    }
    assertFalse(res);

    // if exponent=0, always schedules
    assigner.setExponent(0.0);
    res = true;
    for (int i = 0; i < numOfTests; i++) {
      res = res & assigner.isScheduled(set, 0);
      res = res & assigner.isScheduled(set, 1);
      res = res & assigner.isScheduled(set, 2);
    }
    assertTrue(res);
  }
}
