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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.scheduler.offers.HostOffer;
import org.apache.aurora.scheduler.offers.Offers;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.NONE;
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
    httpOfferSet = new HttpOfferSetImpl(new HashSet<>());
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
}
