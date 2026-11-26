package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.ejb.Local;
import javax.ejb.LocalBean;
import javax.enterprise.event.Event;
import javax.enterprise.event.Observes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.configuration.ConfigurationBean;
import datawave.configuration.ConfigurationEvent;
import datawave.configuration.RefreshEvent;
import datawave.configuration.RefreshLifecycle;
import datawave.configuration.spring.ThreadSafeClassPathXmlApplicationContext;

class QueryLimiterRefreshTest {

    @TempDir
    private Path directory;

    private TestingServer server;
    private FileSystemXmlApplicationContext context;
    private QueryLimiterImpl limiter;
    private ConfigurationBean configuration;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
        context = new FileSystemXmlApplicationContext();
        context.setConfigLocation(directory.resolve("limiter.xml").toUri().toString());
        limiter = new QueryLimiterImpl();
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            limiter.close();
        } finally {
            try {
                context.close();
            } finally {
                server.close();
            }
        }
    }

    @Test
    void appliesUpdatedUserAndSystemLimitsWithoutDroppingQueries() throws Exception {
        start(QueryLimiterMode.ENFORCE, 1, 10);
        limiter.markActive("query", "user", "system", "logic");
        CuratorFramework client = client();
        QueryHeartbeatCache cache = cache();
        QueryHeartbeat heartbeat = cache.get("query");
        assertThat(limiter.checkLimits("user", "system", "logic").metLimit()).isTrue();

        refresh(QueryLimiterMode.ENFORCE, 2, 10);
        assertThat(limiter.checkLimits("user", "system", "logic").metLimit()).isFalse();
        assertThat(client()).isSameAs(client);
        assertThat(cache()).isSameAs(cache);
        assertThat(cache.get("query")).isSameAs(heartbeat);

        refresh(QueryLimiterMode.ENFORCE, 10, 1);
        QueryLimiterResponse response = limiter.checkLimits("user", "system", "logic");
        assertThat(response.metLimit()).isTrue();
        assertThat(response.getMessage()).contains("System 'system'");
        assertThat(limiter.getActiveQueries()).containsExactly("query");
    }

    @ParameterizedTest
    @CsvSource({"ENFORCE, TRACK_ONLY", "TRACK_ONLY, ENFORCE", "ENFORCE, ENFORCE", "TRACK_ONLY, TRACK_ONLY"})
    void preservesHeartbeatsAcrossTrackingModeRefreshes(QueryLimiterMode initialMode, QueryLimiterMode refreshedMode) throws Exception {
        start(initialMode, 1, 10);
        limiter.markActive("query", "user", "system", "logic");
        CuratorFramework client = client();
        QueryHeartbeatCache cache = cache();
        QueryHeartbeat heartbeat = cache.get("query");

        refresh(refreshedMode, 1, 10);

        assertThat(client()).isSameAs(client);
        assertThat(cache()).isSameAs(cache);
        assertThat(cache.get("query")).isSameAs(heartbeat);
        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STARTED);
        assertThat(limiter.checkLimits("user", "system", "logic").metLimit()).isEqualTo(refreshedMode == QueryLimiterMode.ENFORCE);
        limiter.markActive("another-query", "user", "system", "logic");
        assertThat(limiter.getActiveQueries()).containsExactlyInAnyOrder("query", "another-query");
    }

    @ParameterizedTest
    @EnumSource(value = QueryLimiterMode.class, names = {"TRACK_ONLY", "ENFORCE"})
    void initializesTrackingWhenEnabledByRefresh(QueryLimiterMode mode) throws Exception {
        start(QueryLimiterMode.DISABLED, 1, 10);
        limiter.markActive("untracked-query", "user", "system", "logic");
        assertThat(client()).isNull();
        assertThat(cache()).isNull();

        refresh(mode, 1, 10);
        limiter.markActive("tracked-query", "user", "system", "logic");

        assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.READY);
        assertThat(limiter.getActiveQueries()).containsExactly("tracked-query");
        assertThat(limiter.checkLimits("user", "system", "logic").metLimit()).isEqualTo(mode == QueryLimiterMode.ENFORCE);
    }

    @ParameterizedTest
    @EnumSource(value = QueryLimiterMode.class, names = {"TRACK_ONLY", "ENFORCE"})
    void closesTrackingWhenDisabledAndReinitializesWhenEnabled(QueryLimiterMode mode) throws Exception {
        start(mode, 1, 10);
        limiter.markActive("query", "user", "system", "logic");
        CuratorFramework client = client();
        QueryHeartbeatCache cache = cache();
        QueryHeartbeat heartbeat = cache.get("query");
        ScheduledExecutorService executor = (ScheduledExecutorService) ReflectionTestUtils.getField(cache, "executor");

        refresh(QueryLimiterMode.DISABLED, 1, 10);

        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STOPPED);
        assertThat(executor.isTerminated()).isTrue();
        assertThat(heartbeat.isStopped()).isTrue();
        assertThat(client()).isNull();
        assertThat(cache()).isNull();
        assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.READY);
        limiter.markActive("untracked-query", "user", "system", "logic");
        assertThat(limiter.getActiveQueries()).isEmpty();
        assertThat(limiter.checkLimits("user", "system", "logic").metLimit()).isFalse();

        refresh(mode, 1, 10);
        assertThat(client()).isNotSameAs(client);
        assertThat(cache()).isNotSameAs(cache);
        assertThat(limiter.getActiveQueries()).isEmpty();
        limiter.markActive("resumed-query", "user", "system", "logic");
        assertThat(limiter.getActiveQueries()).containsExactly("resumed-query");
    }

    @Test
    void recoversFromInvalidRefreshWithoutLosingTrackedQueries() throws Exception {
        start(QueryLimiterMode.ENFORCE, 1, 10);
        limiter.markActive("query", "user", "system", "logic");
        CuratorFramework client = client();
        QueryHeartbeatCache cache = cache();

        assertThatThrownBy(() -> refresh(QueryLimiterMode.ENFORCE, 0, 10)).hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.FAILED);
        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STARTED);
        assertThat(cache.get("query")).isNotNull();

        refresh(QueryLimiterMode.ENFORCE, 1, 10);
        assertThat(client()).isSameAs(client);
        assertThat(cache()).isSameAs(cache);
        assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.READY);
        assertThat(limiter.checkLimits("user", "system", "logic").metLimit()).isTrue();
    }

    @Test
    void rejectsNullModeOnRefreshWithoutDroppingTrackedQueries() throws Exception {
        start(QueryLimiterMode.ENFORCE, 1, 10);
        limiter.markActive("query", "user", "system", "logic");
        CuratorFramework client = client();
        context.getBean("queryLimiterConfig", QueryLimiterImplConfiguration.class).setMode(null);

        assertThatThrownBy(() -> limiter.onRefresh(RefreshLifecycle.COMPLETE)).hasRootCauseInstanceOf(NullPointerException.class)
                        .hasRootCauseMessage("Limiter mode cannot be null");
        assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.FAILED);
        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STARTED);
        assertThat(cache().get("query")).isNotNull();

        refresh(QueryLimiterMode.ENFORCE, 1, 10);
        assertThat(client()).isSameAs(client);
        assertThat(limiter.checkLimits("user", "system", "logic").metLimit()).isTrue();
    }

    @Test
    void keepsAppliedConfigurationIndependentOfSpringBeanMutations() throws Exception {
        start(QueryLimiterMode.ENFORCE, 1, 10);
        limiter.markActive("query", "user", "system", "logic");
        refresh(QueryLimiterMode.ENFORCE, 1, 10);
        QueryLimiterImplConfiguration springConfig = context.getBean("queryLimiterConfig", QueryLimiterImplConfiguration.class);
        springConfig.setMode(QueryLimiterMode.DISABLED);
        springConfig.getLimitConfiguration().setDefaultUserQueryLimit(10);

        assertThat(limiter.checkLimits("user", "system", "logic").metLimit()).isTrue();
        assertThat(limiter.getActiveQueries()).containsExactly("query");
    }

    @Test
    void ignoresRefreshInitiationAndDoesNotReactivateAfterClose() throws Exception {
        limiter.onRefresh(RefreshLifecycle.INITIATED);
        assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.UNINITIALIZED);
        start(QueryLimiterMode.DISABLED, 1, 10);
        limiter.close();

        refresh(QueryLimiterMode.ENFORCE, 1, 10);

        assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.CLOSED);
        assertThat(client()).isNull();
        assertThat(cache()).isNull();
    }

    @Test
    void exposesObserverThroughAnEjbBusinessView() throws Exception {
        Method observer = QueryLimiterImpl.class.getMethod("onRefresh", RefreshLifecycle.class);
        assertThat(observer.getParameters()[0].isAnnotationPresent(Observes.class)).isTrue();
        assertThat(QueryLimiterImpl.class.isAnnotationPresent(LocalBean.class)).isTrue();
        assertThat(QueryLimiterImpl.class.getAnnotation(Local.class).value()).contains(QueryLimiter.class);
    }

    @SuppressWarnings("unchecked")
    private void start(QueryLimiterMode mode, int userLimit, int systemLimit) throws Exception {
        writeConfiguration(mode, userLimit, systemLimit);
        context.refresh();
        ConfigurableApplicationContext applicationContext = new ThreadSafeClassPathXmlApplicationContext(context, new ReentrantReadWriteLock());
        ReflectionTestUtils.setField(limiter, "applicationContext", applicationContext);
        ReflectionTestUtils.setField(limiter, "config", context.getBean("queryLimiterConfig"));
        limiter.setup();

        configuration = new ConfigurationBean();
        Event<ConfigurationEvent> configurationEvents = mock(Event.class);
        doAnswer(invocation -> {
            applicationContext.refresh();
            return null;
        }).when(configurationEvents).fire(any(ConfigurationEvent.class));
        Event<RefreshEvent> refreshEvents = mock(Event.class);
        Event<RefreshLifecycle> lifecycleEvents = mock(Event.class);
        doAnswer(invocation -> {
            limiter.onRefresh(invocation.getArgument(0, RefreshLifecycle.class));
            return null;
        }).when(lifecycleEvents).fire(any(RefreshLifecycle.class));
        ReflectionTestUtils.setField(configuration, "configurationEvent", configurationEvents);
        ReflectionTestUtils.setField(configuration, "refreshEvent", refreshEvents);
        ReflectionTestUtils.setField(configuration, "refreshLifecycleEvent", lifecycleEvents);
    }

    private void refresh(QueryLimiterMode mode, int userLimit, int systemLimit) throws Exception {
        writeConfiguration(mode, userLimit, systemLimit);
        configuration.refreshInternal();
    }

    private void writeConfiguration(QueryLimiterMode mode, int userLimit, int systemLimit) throws Exception {
        String xml = "<beans xmlns='http://www.springframework.org/schema/beans' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                        + "xsi:schemaLocation='http://www.springframework.org/schema/beans https://www.springframework.org/schema/beans/spring-beans.xsd'>"
                        + "<bean id='builder' class='datawave.core.common.zookeeper.ZkClientBuilder'>" + "<property name='connectString' value='"
                        + server.getConnectString() + "'/></bean>" + "<bean id='limits' class='datawave.webservice.query.limit.QueryLimitConfiguration'>"
                        + "<property name='defaultUserQueryLimit' value='" + userLimit + "'/>" + "<property name='defaultSystemQueryLimit' value='"
                        + systemLimit + "'/></bean>" + "<bean id='queryLimiterConfig' class='datawave.webservice.query.limit.QueryLimiterImplConfiguration'>"
                        + "<property name='mode' value='" + mode + "'/><property name='zkClientBuilder' ref='builder'/>"
                        + "<property name='limitConfiguration' ref='limits'/></bean></beans>";
        Files.writeString(directory.resolve("limiter.xml"), xml);
    }

    private CuratorFramework client() {
        return (CuratorFramework) ReflectionTestUtils.getField(limiter, "zkClient");
    }

    private QueryHeartbeatCache cache() {
        return (QueryHeartbeatCache) ReflectionTestUtils.getField(limiter, "heartbeatCache");
    }
}
