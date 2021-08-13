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
package org.apache.aurora.scheduler.discovery;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.common.zookeeper.testing.BaseZooKeeperTest;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;
import org.apache.aurora.scheduler.discovery.ServiceInstance.Endpoint;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.junit.Before;

class BaseCuratorDiscoveryTest extends BaseZooKeeperTest {

  static final String GROUP_PATH = "/group/root";
  static final String MEMBER_TOKEN = "member_";
  static final int PRIMARY_PORT = 42;

  private CuratorFramework client;
  private BlockingQueue<PathChildrenCacheEvent> groupEvents;
  private CuratorServiceGroupMonitor groupMonitor;

  @Before
  public void setUpCurator() {
    client = startNewClient();
    CuratorCache groupCache = CuratorCache.builder(client, GROUP_PATH).build();
    groupEvents = new LinkedBlockingQueue<>();
    CuratorCacheListener listener = CuratorCacheListener.builder()
            .forPathChildrenCache(GROUP_PATH, client, (c, event) -> groupEvents.put(event)).build();
    groupCache.listenable().addListener(listener);
    Predicate<String> memberSelector = name -> name.contains(MEMBER_TOKEN);
    groupMonitor = new CuratorServiceGroupMonitor(groupCache, memberSelector);
  }

  final CuratorFramework startNewClient() {
    CuratorFramework curator = CuratorFrameworkFactory.builder()
        .retryPolicy((retryCount, elapsedTimeMs, sleeper) -> false) // Don't retry.
        .connectString(String.format("localhost:%d", getServer().getPort()))
        .build();
    curator.start();
    addTearDown(curator::close);
    return curator;
  }

  final void expireSession(CuratorFramework curator) throws Exception {
    getServer().expireClientSession(curator.getZookeeperClient().getZooKeeper().getSessionId());
  }

  final void causeDisconnection() throws Exception {
    getServer().stop();
    getServer().restartNetwork();
  }

  final CuratorFramework getClient() {
    return client;
  }

  final CuratorServiceGroupMonitor getGroupMonitor() {
    return groupMonitor;
  }

  final void startGroupMonitor() throws ServiceGroupMonitor.MonitorException {
    groupMonitor.start();
    addTearDown(groupMonitor::close);
  }

  final void expectGroupEvent(PathChildrenCacheEvent.Type eventType) {
    while (true) {
      try {
        PathChildrenCacheEvent event = groupEvents.take();
        if (event.getType() == eventType) {
          break;
        }
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  final ServiceInstance serviceInstance(String hostName) {
    return new ServiceInstance(new Endpoint(hostName, PRIMARY_PORT), ImmutableMap.of());
  }
}
