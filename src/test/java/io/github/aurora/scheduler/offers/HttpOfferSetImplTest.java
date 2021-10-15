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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.Offers;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
import static org.apache.aurora.scheduler.base.TaskTestUtil.JOB;
import static org.apache.aurora.scheduler.base.TaskTestUtil.makeTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpOfferSetImplTest extends EasyMockTest {

  private static final String HOST_A = "HOST_A";
  private static final IHostAttributes HOST_ATTRIBUTES_A =
      IHostAttributes.build(new HostAttributes().setMode(NONE).setHost(HOST_A));
  private static final HostOffer OFFER_A = new HostOffer(
      Offers.makeOffer("OFFER_A", HOST_A),
      HOST_ATTRIBUTES_A);
  private static final String HOST_B = "HOST_B";
  private static final IHostAttributes HOST_ATTRIBUTES_B =
          IHostAttributes.build(new HostAttributes().setMode(NONE).setHost(HOST_B));
  private static final HostOffer OFFER_B = new HostOffer(
      Offers.makeOffer("OFFER_B", HOST_B),
          HOST_ATTRIBUTES_B);
  private static final String HOST_C = "HOST_C";
  private static final HostOffer OFFER_C = new HostOffer(
      Offers.makeOffer("OFFER_C", HOST_C),
      IHostAttributes.build(new HostAttributes().setMode(NONE).setHost(HOST_C)));
  private static final String HOST_D = "HOST_D";

  private HttpOfferSetImpl httpOfferSet;

  @Before
  public void setUp() throws IOException {
    Storage storage = MemStorageModule.newEmptyStorage();
    storage.write((Storage.MutateWork.NoResult.Quiet) sp -> {
      ScheduledTask t0 = makeTask("t0", JOB)
          .newBuilder()
          .setStatus(ScheduleStatus.PENDING);

      ScheduledTask t1 = makeTask("t1", JOB)
          .newBuilder()
          .setStatus(ScheduleStatus.STARTING);
      t1.setAssignedTask(new AssignedTask("t1",
              OFFER_B.getOffer().getAgentId().getValue(),
              OFFER_B.getOffer().getHostname(),
              t1.getAssignedTask().getTask(),
              new HashMap<>(),
              0));

      ScheduledTask t2 = makeTask("t2", JOB)
          .newBuilder()
          .setStatus(ScheduleStatus.RUNNING);
      t2.setAssignedTask(new AssignedTask("t2",
          OFFER_C.getOffer().getAgentId().getValue(),
          OFFER_C.getOffer().getHostname(),
          t1.getAssignedTask().getTask(),
          new HashMap<>(),
          0));

      sp.getUnsafeTaskStore().saveTasks(
          IScheduledTask.setFromBuilders(ImmutableList.of(t0, t1, t2)));
    });

    httpOfferSet = new HttpOfferSetImpl(new HashSet<>(),
        storage,
        0,
        new URL("http://localhost:9090/v1/offerset"),
        0,
        1);
    httpOfferSet.add(OFFER_A);
    httpOfferSet.add(OFFER_B);
    httpOfferSet.add(OFFER_C);
  }

  @Test
  public void testProcessResponse() throws IOException {
    control.replay();
    String responseStr = "{\"error\": \"\", \"hosts\": [\""
            + HOST_A + "\",\""
            + HOST_B + "\",\""
            + HOST_C + "\"]}";
    List<Long> offerSetDiffList = new LinkedList<>();

    List<HostOffer> sortedOffers = httpOfferSet.processResponse(responseStr, offerSetDiffList);
    assertEquals(sortedOffers.size(), 3);
    assertEquals(sortedOffers.get(0).getAttributes().getHost(), HOST_A);
    assertEquals(sortedOffers.get(1).getAttributes().getHost(), HOST_B);
    assertEquals(sortedOffers.get(2).getAttributes().getHost(), HOST_C);
    assertEquals((long) offerSetDiffList.get(0), 0);

    // plugin returns less offers than Aurora has.
    responseStr = "{\"error\": \"\", \"hosts\": [\""
            + HOST_A + "\",\""
            + HOST_C + "\"]}";
    sortedOffers = httpOfferSet.processResponse(responseStr, offerSetDiffList);
    assertEquals(sortedOffers.size(), 2);
    assertEquals(sortedOffers.get(0).getAttributes().getHost(), HOST_A);
    assertEquals(sortedOffers.get(1).getAttributes().getHost(), HOST_C);
    assertEquals((long) offerSetDiffList.get(1), 1);

    // plugin returns more offers than Aurora has.
    responseStr = "{\"error\": \"\", \"hosts\": [\""
            + HOST_A + "\",\""
            + HOST_B + "\",\""
            + HOST_D + "\",\""
            + HOST_C + "\"]}";
    sortedOffers = httpOfferSet.processResponse(responseStr, offerSetDiffList);
    assertEquals(sortedOffers.size(), 3);
    assertEquals(sortedOffers.get(0).getAttributes().getHost(), HOST_A);
    assertEquals(sortedOffers.get(1).getAttributes().getHost(), HOST_B);
    assertEquals(sortedOffers.get(2).getAttributes().getHost(), HOST_C);
    assertEquals((long) offerSetDiffList.get(2), 1);

    // plugin omits 1 offer & returns 1 extra offer
    responseStr = "{\"error\": \"\", \"hosts\": [\""
        + HOST_A + "\",\""
        + HOST_D + "\",\""
        + HOST_C + "\"]}";
    sortedOffers = httpOfferSet.processResponse(responseStr, offerSetDiffList);
    assertEquals(sortedOffers.size(), 2);
    assertEquals(sortedOffers.get(0).getAttributes().getHost(), HOST_A);
    assertEquals(sortedOffers.get(1).getAttributes().getHost(), HOST_C);
    assertEquals((long) offerSetDiffList.get(3), 2);

    responseStr = "{\"error\": \"Error\", \"hosts\": [\""
            + HOST_A + "\",\""
            + HOST_B + "\",\""
            + HOST_C + "\"]}";
    boolean isException = false;
    try {
      httpOfferSet.processResponse(responseStr, offerSetDiffList);
    } catch (IOException ioe) {
      isException = true;
    }
    assertTrue(isException);

    responseStr = "{\"error\": \"error\"}";
    isException = false;
    try {
      httpOfferSet.processResponse(responseStr, new LinkedList<>());
    } catch (IOException ioe) {
      isException = true;
    }
    assertTrue(isException);

    responseStr = "{\"weird\": \"cannot decode this json string\"}";
    isException = false;
    try {
      httpOfferSet.processResponse(responseStr, new LinkedList<>());
    } catch (IOException ioe) {
      isException = true;
    }
    assertTrue(isException);
  }

  @Test
  public void testGetOrdered() {
    control.replay();

    // OFFER_B is put in the bottom of list as it has 1 starting task.
    IScheduledTask task = makeTask("id", JOB);
    Iterable<HostOffer> sortedOffers = httpOfferSet.getOrdered(
        TaskGroupKey.from(task.getAssignedTask().getTask()),
        TaskTestUtil.toResourceRequest(task.getAssignedTask().getTask()));

    assertEquals(3, Iterables.size(sortedOffers));
    HostOffer lastOffer = null;
    for (HostOffer o: sortedOffers) {
      lastOffer = o;
    }
    assertEquals(OFFER_B, lastOffer);
  }
}
