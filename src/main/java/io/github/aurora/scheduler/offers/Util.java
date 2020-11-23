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

import java.util.LinkedList;
import java.util.List;

import com.google.common.math.Quantiles;

import org.apache.aurora.scheduler.offers.HostOffer;

public final class Util {

  private Util() {
    // Utility class.
  }

  public static Number percentile(List<Long> list, double percentile) {
    if (list.isEmpty()) {
      return 0.0;
    }

    // index should be a full integer. use quantile scale to allow reporting of percentile values
    // such as p99.9.
    double percentileCopy = percentile;
    int quantileScale = 100;
    while ((percentileCopy - Math.floor(percentileCopy)) > 0) {
      quantileScale *= 10;
      percentileCopy *= 10;
    }

    return Quantiles.scale(quantileScale).index((int) Math.floor(quantileScale - percentileCopy))
            .compute(list);
  }

  public static long max(List<Long> list) {
    long max = 0;
    for (long e : list) {
      if (e > max) {
        max = e;
      }
    }
    return max;
  }

  public static long avg(List<Long> list) {
    if (list.isEmpty()) {
      return 0;
    }

    long avg = 0;
    for (long e : list) {
      avg += e;
    }
    return avg / list.size();
  }

  public static List<String> getHostnames(Iterable<HostOffer> offers) {
    List<String> hostnames = new LinkedList<>();
    for (HostOffer offer: offers) {
      hostnames.add(offer.getOffer().getHostname());
    }
    return hostnames;
  }
}
