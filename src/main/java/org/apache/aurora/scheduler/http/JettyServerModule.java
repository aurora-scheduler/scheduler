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
package org.apache.aurora.scheduler.http;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.DispatcherType;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextListener;
import javax.ws.rs.HttpMethod;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.ServletModule;
import com.google.inject.util.Modules;

import org.apache.aurora.common.net.http.handlers.AbortHandler;
import org.apache.aurora.common.net.http.handlers.ContentionPrinter;
import org.apache.aurora.common.net.http.handlers.HealthHandler;
import org.apache.aurora.common.net.http.handlers.QuitHandler;
import org.apache.aurora.common.net.http.handlers.ThreadStackPrinter;
import org.apache.aurora.common.net.http.handlers.TimeSeriesDataSource;
import org.apache.aurora.common.net.http.handlers.VarsHandler;
import org.apache.aurora.common.net.http.handlers.VarsJsonHandler;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor.MonitorException;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.http.api.ApiModule;
import org.apache.aurora.scheduler.http.api.security.HttpSecurityModule;
import org.apache.aurora.scheduler.thrift.ThriftModule;
import org.apache.aurora.scheduler.thrift.aop.AopModule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.jboss.resteasy.plugins.guice.GuiceResteasyBootstrapServletContextListener;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for scheduler HTTP servlets.
 * <p>
 * TODO(wfarner): Continue improvements here by simplifying serving of static assets.  Jetty's
 * DefaultServlet can take over this responsibility, and jetty-rewite can be used to rewrite
 * requests (for static assets) similar to what we currently do with path specs.
 */
public class JettyServerModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(JettyServerModule.class);

  // The name of the request attribute where the path for the current request before it was
  // rewritten is stored.
  static final String ORIGINAL_PATH_ATTRIBUTE_NAME = "originalPath";

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-hostname",
        description =
            "The hostname to advertise in ZooKeeper instead of the locally-resolved hostname.")
    public String hostnameOverride;

    @Parameter(names = "-http_port",
        description =
            "The port to start an HTTP server on.  Default value will choose a random port.")
    public int httpPort = 0;

    @Parameter(names = "-ip",
        description =
            "The ip address to listen. If not set, the scheduler will listen on all interfaces.")
    public String listenIp;
  }

  private static final String STATIC_ASSETS_ROOT = Resource
      .newClassPathResource("scheduler/assets/index.html")
      .toString()
      .replace("assets/index.html", "");

  private final CliOptions options;
  private final boolean production;

  public JettyServerModule(CliOptions options) {
    this(options, true);
  }

  @VisibleForTesting
  JettyServerModule(CliOptions options, boolean production) {
    this.options = options;
    this.production = production;
  }

  @Override
  protected void configure() {
    bind(Runnable.class)
        .annotatedWith(Names.named(AbortHandler.ABORT_HANDLER_KEY))
        .to(AbortCallback.class);
    bind(AbortCallback.class).in(Singleton.class);
    bind(Runnable.class).annotatedWith(Names.named(QuitHandler.QUIT_HANDLER_KEY))
        .to(QuitCallback.class);
    bind(QuitCallback.class).in(Singleton.class);
    bind(new TypeLiteral<Supplier<Boolean>>() { })
        .annotatedWith(Names.named(HealthHandler.HEALTH_CHECKER_KEY))
        .toInstance(Suppliers.ofInstance(true));

    final Optional<String> hostnameOverride =
        Optional.ofNullable(options.jetty.hostnameOverride);
    if (hostnameOverride.isPresent()) {
      try {
        InetAddress.getByName(hostnameOverride.get());
      } catch (UnknownHostException e) {
        /* Possible misconfiguration, so warn the user. */
        LOG.warn("Unable to resolve name specified in -hostname. "
                    + "Depending on your environment, this may be valid.");
      }
    }
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<Optional<String>>() { }).toInstance(hostnameOverride);
        bind(HttpService.class).to(HttpServerLauncher.class);
        bind(HttpServerLauncher.class).in(Singleton.class);
        expose(HttpServerLauncher.class);
        expose(HttpService.class);
      }
    });
    SchedulerServicesModule.addAppStartupServiceBinding(binder()).to(HttpServerLauncher.class);

    bind(LeaderRedirect.class).in(Singleton.class);
    SchedulerServicesModule.addAppStartupServiceBinding(binder()).to(RedirectMonitor.class);

    if (production) {
      install(new AbstractModule() {
        @Override
        protected void configure() {
          // Provider binding only.
        }

        @Provides
        @Singleton
        ServletContextListener provideServletContextListener(Injector parentInjector) {
          return makeServletContextListener(
              parentInjector,
              (servletContext) -> Modules.combine(
                  new ApiModule(options.api),
                  new HttpSecurityModule(options, servletContext),
                  new ThriftModule(),
                  new AopModule(options)));
        }
      });
    }
  }

  private static final Set<String> LEADER_ENDPOINTS = ImmutableSet.of(
      "agents",
      "api",
      "cron",
      "maintenance",
      "mname",
      "offers",
      "pendingtasks",
      "quotas",
      "slaves",
      "state",
      "tiers",
      "utilization"
  );

  private static final Multimap<Class<?>, String> JAX_RS_ENDPOINTS =
      ImmutableMultimap.<Class<?>, String>builder()
          .put(AbortHandler.class, "abortabortabort")
          .put(Agents.class, "agents")
          .put(ContentionPrinter.class, "contention")
          .put(Cron.class, "cron")
          .put(HealthHandler.class, "health")
          .put(LeaderHealth.class, "leaderhealth")
          .put(LogConfig.class, "logconfig")
          .put(Maintenance.class, "maintenance")
          .put(Mname.class, "mname")
          .put(Offers.class, "offers")
          .put(PendingTasks.class, "pendingtasks")
          .put(QuitHandler.class, "quitquitquit")
          .put(Quotas.class, "quotas")
          .put(Services.class, "services")
          .put(StructDump.class, "structdump")
          .put(State.class, "state")
          .put(ThreadStackPrinter.class, "threads")
          .put(Tiers.class, "tiers")
          .put(TimeSeriesDataSource.class, "graphdata")
          .put(Utilization.class, "utilization")
          .put(VarsHandler.class, "vars")
          .put(VarsJsonHandler.class, "vars.json")
          .build();

  private static String allOf(Set<String> paths) {
    return "^(?:"
        + Joiner.on("|").join(Iterables.transform(paths, path -> "/" + path))
        + ").*$";
  }

  // TODO(ksweeney): Factor individual servlet configurations to their own ServletModules.
  @VisibleForTesting
  static ServletContextListener makeServletContextListener(
      Injector parentInjector,
      Function<ServletContext, Module> childModuleFactory) {

    ServletContextListener contextListener = new GuiceResteasyBootstrapServletContextListener() {
      @Override
      protected List<? extends Module> getModules(ServletContext context) {
        return ImmutableList.of(
            childModuleFactory.apply(context),
            new ServletModule() {
              @Override
              protected void configureServlets() {
                bind(HttpStatsFilter.class).in(Singleton.class);
                filter("*").through(HttpStatsFilter.class);

                bind(LeaderRedirectFilter.class).in(Singleton.class);
                filterRegex(allOf(LEADER_ENDPOINTS))
                    .through(LeaderRedirectFilter.class);

                filterRegex("/assets/scheduler(?:/.*)?").through(LeaderRedirectFilter.class);

                serve("/assets", "/assets/*")
                    .with(new DefaultServlet(), ImmutableMap.of(
                        "cacheControl", "max-age=3600",
                        "resourceBase", STATIC_ASSETS_ROOT,
                        "dirAllowed", "false"));

                for (Class<?> jaxRsHandler : JAX_RS_ENDPOINTS.keySet()) {
                  bind(jaxRsHandler);
                }
              }
            }
        );
      }
    };

    // Injects the Injector into GuiceResteasyBootstrapServletContextListener.
    parentInjector.injectMembers(contextListener);

    return contextListener;
  }

  static class RedirectMonitor extends AbstractIdleService {
    private final LeaderRedirect redirector;

    @Inject
    RedirectMonitor(LeaderRedirect redirector) {
      this.redirector = requireNonNull(redirector);
    }

    @Override
    public void startUp() throws MonitorException {
      redirector.monitor();
    }

    @Override
    protected void shutDown() throws IOException {
      redirector.close();
    }
  }

  public static final class HttpServerLauncher extends AbstractIdleService implements HttpService {
    private final CliOptions options;
    private final ServletContextListener servletContextListener;
    private final Optional<String> advertisedHostOverride;
    private volatile Server server;
    private volatile HostAndPort serverAddress = null;

    @Inject
    HttpServerLauncher(
        CliOptions options,
        ServletContextListener servletContextListener,
        Optional<String> advertisedHostOverride) {

      this.options = requireNonNull(options);
      this.servletContextListener = requireNonNull(servletContextListener);
      this.advertisedHostOverride = requireNonNull(advertisedHostOverride);
    }

    private static final Map<String, String> REGEX_REWRITE_RULES =
        ImmutableMap.<String, String>builder()
            .put("/(?:index.html)?", "/assets/index.html")
            .put("/graphview(?:/index.html)?", "/assets/graphview/graphview.html")
            .put("/graphview/(.*)", "/assets/graphview/$1")
            .put("/(?:scheduler|updates)(?:/.*)?", "/assets/scheduler/index.html")
            .put("/slaves", "/agents")
            .build();

    private static Handler getRewriteHandler(Handler wrapped) {
      RewriteHandler rewrites = new RewriteHandler();
      rewrites.setOriginalPathAttribute(ORIGINAL_PATH_ATTRIBUTE_NAME);
      rewrites.setRewriteRequestURI(true);
      rewrites.setRewritePathInfo(true);

      for (Map.Entry<String, String> entry : REGEX_REWRITE_RULES.entrySet()) {
        RewriteRegexRule rule = new RewriteRegexRule();
        rule.setRegex(entry.getKey());
        rule.setReplacement(entry.getValue());
        rewrites.addRule(rule);
      }

      rewrites.setHandler(wrapped);

      return rewrites;
    }

    private static Handler getGzipHandler(Handler wrapped) {
      GzipHandler gzip = new GzipHandler();
      gzip.addIncludedMethods(HttpMethod.POST);
      gzip.setHandler(wrapped);
      return gzip;
    }

    @Override
    public HostAndPort getAddress() {
      Preconditions.checkState(state() == State.RUNNING);
      return HostAndPort.fromParts(
          advertisedHostOverride.orElse(serverAddress.getHost()),
          serverAddress.getPort());
    }

    @Override
    protected void startUp() {
      server = new Server();
      ServletContextHandler servletHandler =
          new ServletContextHandler(server, "/", ServletContextHandler.NO_SESSIONS);

      servletHandler.addServlet(DefaultServlet.class, "/");
      servletHandler.addFilter(GuiceFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
      servletHandler.addEventListener(servletContextListener);
      servletHandler.addServlet(HttpServletDispatcher.class, "/*");

      HandlerCollection rootHandler = new HandlerList();

      RequestLogHandler logHandler = new RequestLogHandler();
      logHandler.setRequestLog(new CustomRequestLog(new Slf4jRequestLogWriter(),
              CustomRequestLog.EXTENDED_NCSA_FORMAT));

      rootHandler.addHandler(logHandler);
      rootHandler.addHandler(servletHandler);

      ServerConnector connector = new ServerConnector(server);
      connector.setPort(options.jetty.httpPort);
      if (options.jetty.listenIp != null) {
        connector.setHost(options.jetty.listenIp);
      }

      server.addConnector(connector);
      server.setHandler(getGzipHandler(getRewriteHandler(rootHandler)));

      try {
        connector.open();
        server.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      String host;
      if (connector.getHost() == null) {
        // Resolve the local host name.
        try {
          host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
          throw new RuntimeException("Failed to resolve local host address: " + e, e);
        }
      } else {
        // If jetty was configured with a specific host to bind to, use that.
        host = connector.getHost();
      }
      serverAddress = HostAndPort.fromParts(host, connector.getLocalPort());
    }

    @Override
    protected void shutDown() {
      LOG.info("Shutting down embedded http server");
      try {
        server.stop();
      } catch (Exception e) {
        LOG.info("Failed to stop jetty server: " + e, e);
      }
    }
  }
}
