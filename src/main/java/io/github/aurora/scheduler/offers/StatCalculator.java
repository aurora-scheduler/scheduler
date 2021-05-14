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

import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.aurora.common.stats.StatsProvider;

import static java.util.Objects.requireNonNull;

public class StatCalculator implements Runnable {
  private final LoadingCache<String, Counter> metricCache;

  private static class Counter implements Supplier<Number> {
    private final AtomicReference<Number> value = new AtomicReference<>((Number) 0);
    private final StatsProvider statsProvider;
    private boolean exported;

    Counter(StatsProvider statsProvider) {
      this.statsProvider = statsProvider;
    }

    @Override
    public Number get() {
      return value.get();
    }

    private void set(String name, Number newValue) {
      if (!exported) {
        statsProvider.makeGauge(name, this);
        exported = true;
      }
      value.set(newValue);
    }
  }

  @Inject
  StatCalculator(final StatsProvider statsProvider) {
    requireNonNull(statsProvider);
    this.metricCache = CacheBuilder.newBuilder().build(
        new CacheLoader<String, StatCalculator.Counter>() {
          public StatCalculator.Counter load(String key) {
            return new StatCalculator.Counter(statsProvider.untracked());
          }
        });
  }

  @Override
  public void run() {
    float medianLatency =
            Util.percentile(HttpOfferSetModule.latencyMsList, 50.0)
                              .floatValue() / 1000000;
    float avgLatency =
            (float) Util.avg(HttpOfferSetModule.latencyMsList) / 1000000;
    float worstLatency =
            (float) Util.max(HttpOfferSetModule.latencyMsList) / 1000000;

    String medianLatencyName = "http_offer_set_median_latency_ms";
    metricCache.getUnchecked(medianLatencyName).set(medianLatencyName, medianLatency);
    String worstLatencyName = "http_offer_set_worst_latency_ms";
    metricCache.getUnchecked(worstLatencyName).set(worstLatencyName, worstLatency);
    String avgLatencyName = "http_offer_set_avg_latency_ms";
    metricCache.getUnchecked(avgLatencyName).set(avgLatencyName, avgLatency);
    String failureCountName = "http_offer_set_failure_count";
    metricCache.getUnchecked(failureCountName).set(failureCountName,
            HttpOfferSetModule.getFailureCount());

    long maxOfferSetDiff = Util.max(HttpOfferSetModule.offerSetDiff);
    String maxOffSetDiffName = "http_offer_set_max_diff";
    metricCache.getUnchecked(maxOffSetDiffName).set(maxOffSetDiffName,
        maxOfferSetDiff);

    // reset the stats.
    HttpOfferSetModule.latencyMsList.clear();
    HttpOfferSetModule.resetFailureCount();
    HttpOfferSetModule.offerSetDiff.clear();
  }
}
