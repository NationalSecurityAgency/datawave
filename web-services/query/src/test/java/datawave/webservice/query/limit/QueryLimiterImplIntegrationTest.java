package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.zookeeper.ZkClientBuilder;

/**
 * Integration tests for {@link QueryLimiterImpl} that tests checking query limits and tracking/untracking queries with a working Zookeeper instance.
 */
class QueryLimiterImplIntegrationTest {

    private TestingServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    @Nested
    @DisplayName("Method setup() throws an exception when")
    class SetupExpectedExceptions {

        @DisplayName("the limiter configuration is null")
        @Test
        void nullLimiterConfiguration() {
            QueryLimiterImpl limiter = new QueryLimiterImpl();

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("Limiter configuration cannot be null");
            // @formatter:on
        }

        @DisplayName("the query limit configuration is null")
        @Test
        void nullQueryLimitConfiguration() {
            QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();

            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(config);

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to initialize limit providers")
                            .getCause()
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("Query limit configuration cannot be null");
            // @formatter:on
        }

        @DisplayName("the default user limit is less than 1")
        @Test
        void defaultUserLimitLessThanOne() {
            QueryLimitConfiguration limitConfig = new QueryLimitConfiguration();
            limitConfig.setDefaultUserQueryLimit(0);

            QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
            config.setLimitConfiguration(limitConfig);

            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(config);

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to initialize limit providers")
                            .getCause()
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Default user query limit must be greater than 0");
            // @formatter:on
        }

        @DisplayName("the internal max cache size is less than 1")
        @Test
        void internalMaxCacheSizeLessThanOne() {
            QueryLimitConfiguration limitConfig = new QueryLimitConfiguration();
            limitConfig.setDefaultUserQueryLimit(100);
            limitConfig.setInternalCacheMaxSize(0);

            QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
            config.setLimitConfiguration(limitConfig);

            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(config);

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to initialize limit providers")
                            .getCause()
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Internal max cache size must be greater than 0");
            // @formatter:on
        }

        @DisplayName("the zookeeper client builder is null")
        @Test
        void nullZkClientBuilder() {
            QueryLimitConfiguration limitConfig = new QueryLimitConfiguration();
            limitConfig.setDefaultUserQueryLimit(100);
            limitConfig.setInternalCacheMaxSize(200);

            QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
            config.setLimitConfiguration(limitConfig);

            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(config);

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to initialize Zookeeper client")
                            .getCause()
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("Zookeeper client builder cannot be null");
            // @formatter:on
        }

        @DisplayName("the zookeeper client connect timeout is less than 1")
        @Test
        void zkClientConnectTimeoutLessThanOne() {
            QueryLimitConfiguration limitConfig = new QueryLimitConfiguration();
            limitConfig.setDefaultUserQueryLimit(100);
            limitConfig.setInternalCacheMaxSize(200);

            QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
            config.setLimitConfiguration(limitConfig);
            config.setZkClientBuilder(new ZkClientBuilder());
            config.setZkClientConnectTimeout(0);

            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(config);

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to initialize Zookeeper client")
                            .getCause()
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Zookeeper client connect timeout must be greater than 0");
            // @formatter:on
        }

        @DisplayName("the zookeeper client connect timeout unit is null")
        @Test
        void nullZkClientConnectTimeoutUnit() {
            QueryLimitConfiguration limitConfig = new QueryLimitConfiguration();
            limitConfig.setDefaultUserQueryLimit(100);
            limitConfig.setInternalCacheMaxSize(200);

            QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
            config.setLimitConfiguration(limitConfig);
            config.setZkClientBuilder(new ZkClientBuilder());
            config.setZkClientConnectTimeoutUnit(null);

            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(config);

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to initialize Zookeeper client")
                            .getCause()
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("Zookeeper client connect timeout unit cannot be null");
            // @formatter:on
        }

        @DisplayName("the heartbeat cleanup interval is less than 1")
        @Test
        void heartbeatCleanupIntervalLessThanOne() {
            QueryLimitConfiguration limitConfig = new QueryLimitConfiguration();
            limitConfig.setDefaultUserQueryLimit(100);
            limitConfig.setInternalCacheMaxSize(200);

            QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
            config.setLimitConfiguration(limitConfig);
            config.setZkClientBuilder(new ZkClientBuilder().withConnectString(server.getConnectString()));
            config.setHeartbeatCleanupInterval(0);

            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(config);

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to initialize heartbeat cache")
                            .getCause()
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("cleanup interval must be greater than 0");
            // @formatter:on
        }

        @DisplayName("the heartbeat cleanup interval unit is null")
        @Test
        void nullHeartbeatCleanupUnit() {
            QueryLimitConfiguration limitConfig = new QueryLimitConfiguration();
            limitConfig.setDefaultUserQueryLimit(100);
            limitConfig.setInternalCacheMaxSize(200);

            QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
            config.setLimitConfiguration(limitConfig);
            config.setZkClientBuilder(new ZkClientBuilder().withConnectString(server.getConnectString()));
            config.setHeartbeatCleanupIntervalUnit(null);

            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(config);

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to initialize heartbeat cache")
                            .getCause()
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("cleanup interval unit must not be null");
            // @formatter:on
        }
    }

    @Nested
    @DisplayName("Method setup() transitions state from")
    class SetupStateTransitions {

        @DisplayName("UNINTITALIZED to UNINITIALIZED after failure")
        @Test
        void failureWithUninitializedState() {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.UNINITIALIZED);

            limiter.setConfiguration(new QueryLimiterImplConfiguration());
            assertThatThrownBy(limiter::setup).isInstanceOf(RuntimeException.class);

            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.UNINITIALIZED);
            assertThat(limiter.isEnabled()).isFalse();
        }

        @DisplayName("UNINTITALIZED to ACTIVE after success")
        @Test
        void successWithUninitializedState() {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.UNINITIALIZED);

            limiter.setConfiguration(createConfig());
            limiter.setup();

            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.ACTIVE);
            assertThat(limiter.isEnabled()).isTrue();

        }

        @DisplayName("ACTIVE to IDLE after failure")
        @Test
        void failureWithActiveState() {
            // Create a query limiter in state ACTIVE.
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());
            limiter.setup();

            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.ACTIVE);
            assertThat(limiter.isEnabled()).isTrue();

            // Call setup with a bad configuration.
            limiter.setConfiguration(new QueryLimiterImplConfiguration());
            assertThatThrownBy(limiter::setup).isInstanceOf(RuntimeException.class);

            // Verify that the limiter is in state IDLE, and is not enabled.
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.IDLE);
            assertThat(limiter.isEnabled()).isFalse();
        }

        @DisplayName("ACTIVE to ACTIVE after success")
        @Test
        void successWithActiveState() {
            // Create a query limiter in state ACTIVE.
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());
            limiter.setup();

            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.ACTIVE);
            assertThat(limiter.isEnabled()).isTrue();

            // Set a different valid configuration, and call setup.
            QueryLimiterImplConfiguration newConfig = createConfig();
            newConfig.getLimitConfiguration().setDefaultUserQueryLimit(500);
            limiter.setConfiguration(newConfig);
            limiter.setup();

            // Verify that the limiter is in state ACTIVE, and is enabled.
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.ACTIVE);
            assertThat(limiter.isEnabled()).isTrue();
        }

        @DisplayName("IDLE to IDLE after failure")
        @Test
        void failureWithIdleState() {
            // Create a query limiter in state ACTIVE.
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());
            limiter.setup();

            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.ACTIVE);
            assertThat(limiter.isEnabled()).isTrue();

            // Call setup with a bad configuration.
            limiter.setConfiguration(new QueryLimiterImplConfiguration());
            assertThatThrownBy(limiter::setup).isInstanceOf(RuntimeException.class);

            // Verify that the limiter is in state IDLE, and is not enabled.
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.IDLE);
            assertThat(limiter.isEnabled()).isFalse();

            // Call setup with a different bad configuration.
            QueryLimiterImplConfiguration newConfig = new QueryLimiterImplConfiguration();
            QueryLimitConfiguration limitConfig = new QueryLimitConfiguration();
            limitConfig.setDefaultUserQueryLimit(-500);
            newConfig.setLimitConfiguration(limitConfig);

            limiter.setConfiguration(newConfig);
            assertThatThrownBy(limiter::setup).isInstanceOf(RuntimeException.class);

            // Verify that the limiter is in state IDLE, and is not enabled.
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.IDLE);
            assertThat(limiter.isEnabled()).isFalse();
        }

        @DisplayName("IDLE to ACTIVE after success")
        @Test
        void successWithIdleState() {
            // Create a query limiter in state ACTIVE.
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());
            limiter.setup();

            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.ACTIVE);
            assertThat(limiter.isEnabled()).isTrue();

            // Call setup with a bad configuration.
            limiter.setConfiguration(new QueryLimiterImplConfiguration());
            assertThatThrownBy(limiter::setup).isInstanceOf(RuntimeException.class);

            // Verify that the limiter is in state IDLE, and is not enabled.
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.IDLE);
            assertThat(limiter.isEnabled()).isFalse();

            // Call setup with a good configuration.
            limiter.setConfiguration(createConfig());
            limiter.setup();

            // Verify that the limiter is in state ACTIVE, and is enabled.
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.ACTIVE);
            assertThat(limiter.isEnabled()).isTrue();
        }

        @DisplayName("CLOSED to CLOSED after failure")
        @Test
        void failureWithClosedState() {
            // Create and shutdown a limiter to put it in the CLOSED state.
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.close();

            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.CLOSED);
            assertThat(limiter.isEnabled()).isFalse();

            // Call setup with a bad configuration.
            limiter.setConfiguration(new QueryLimiterImplConfiguration());
            assertThatThrownBy(limiter::setup).isInstanceOf(RuntimeException.class);

            // Verify that the limiter is in state CLOSED, and is not enabled.
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.CLOSED);
            assertThat(limiter.isEnabled()).isFalse();
        }

        @DisplayName("CLOSED to ACTIVE after success")
        @Test
        void successWithClosedState() {
            // Create a limiter with a good configuration, put it in an active state, and then close it.
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());
            limiter.setup();
            limiter.close();

            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.CLOSED);
            assertThat(limiter.isEnabled()).isFalse();

            // Call setup with a good configuration.
            limiter.setConfiguration(createConfig());
            limiter.setup();

            // Verify that the limiter is in state ACTIVE, and is enabled.
            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.ACTIVE);
            assertThat(limiter.isEnabled()).isTrue();
        }
    }

    @Nested
    @DisplayName("Method setup()")
    class SetupBasicFunctionality {

        @DisplayName("recreates the limit providers after a second successful call")
        @Test
        void limitProvidersAreRecreatedAfterSuccess() throws NoSuchFieldException, IllegalAccessException {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());

            // The providers should be null before setup() is called.
            assertThat(getField(limiter, QueryLogicGroupLimitProvider.class, "queryLogicGroupLimitProvider")).isNull();
            assertThat(getField(limiter, UserLimitProvider.class, "userLimitProvider")).isNull();
            assertThat(getField(limiter, SystemLimitProvider.class, "systemLimitProvider")).isNull();

            // Call setup to initialize the providers.
            limiter.setup();

            QueryLogicGroupLimitProvider groupLimitProvider1 = getField(limiter, QueryLogicGroupLimitProvider.class, "queryLogicGroupLimitProvider");
            UserLimitProvider userLimitProvider1 = getField(limiter, UserLimitProvider.class, "userLimitProvider");
            SystemLimitProvider systemLimitProvider1 = getField(limiter, SystemLimitProvider.class, "systemLimitProvider");

            // The providers should not null after setup() is called.
            assertThat(groupLimitProvider1).isNotNull();
            assertThat(userLimitProvider1).isNotNull();
            assertThat(systemLimitProvider1).isNotNull();

            // Call setup again to reinitialize the providers.
            limiter.setup();

            // Get the new limit providers.
            QueryLogicGroupLimitProvider groupLimitProvider2 = getField(limiter, QueryLogicGroupLimitProvider.class, "queryLogicGroupLimitProvider");
            UserLimitProvider userLimitProvider2 = getField(limiter, UserLimitProvider.class, "userLimitProvider");
            SystemLimitProvider systemLimitProvider2 = getField(limiter, SystemLimitProvider.class, "systemLimitProvider");

            // Verify they are not the same limit providers.
            assertThat(groupLimitProvider2).isNotSameAs(groupLimitProvider1);
            assertThat(userLimitProvider2).isNotSameAs(userLimitProvider1);
            assertThat(systemLimitProvider2).isNotSameAs(systemLimitProvider1);
        }

        @DisplayName("nullifies the limit providers after a second failing call")
        @Test
        void limitProvidersAreNullAfterFailure() throws NoSuchFieldException, IllegalAccessException {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());

            // The providers should be null before setup() is called.
            assertThat(getField(limiter, QueryLogicGroupLimitProvider.class, "queryLogicGroupLimitProvider")).isNull();
            assertThat(getField(limiter, UserLimitProvider.class, "userLimitProvider")).isNull();
            assertThat(getField(limiter, SystemLimitProvider.class, "systemLimitProvider")).isNull();

            // Call setup to initialize the providers.
            limiter.setup();

            // The providers should not be null after the first successful call to setup().
            assertThat(getField(limiter, QueryLogicGroupLimitProvider.class, "queryLogicGroupLimitProvider")).isNotNull();
            assertThat(getField(limiter, UserLimitProvider.class, "userLimitProvider")).isNotNull();
            assertThat(getField(limiter, SystemLimitProvider.class, "systemLimitProvider")).isNotNull();

            // Set the limiter with a bad configuration.
            QueryLimiterImplConfiguration config = createConfig();
            config.setLimitConfiguration(null);
            limiter.setConfiguration(config);

            // Call setup() again to reinitialize the providers.
            assertThatThrownBy(limiter::setup).isInstanceOf(RuntimeException.class);

            // The providers should be null after setup() was called with a bad configuration.
            assertThat(getField(limiter, QueryLogicGroupLimitProvider.class, "queryLogicGroupLimitProvider")).isNull();
            assertThat(getField(limiter, UserLimitProvider.class, "userLimitProvider")).isNull();
            assertThat(getField(limiter, SystemLimitProvider.class, "systemLimitProvider")).isNull();
        }

        @DisplayName("nullifies the zookeeper client after the client connection times out")
        @Test
        void zookeeperClientIsNullAfterConnectionTimeout() throws NoSuchFieldException, IllegalAccessException, IOException {
            // Create the query limiter with a valid configuration with a small timeout for the zookeeper client connection.
            QueryLimiterImplConfiguration config = createConfig();
            config.setZkClientConnectTimeout(10);
            config.setZkClientConnectTimeoutUnit(TimeUnit.MILLISECONDS);

            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(config);

            // Stop the Zookeeper server so a connection can't be established.
            server.stop();

            // @formatter:off
            assertThatThrownBy(limiter::setup)
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to activate limiter")
                            .getCause()
                            .isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to initialize Zookeeper client")
                            .getCause()
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("Zookeeper client failed to connect within timeout of 10 MILLISECONDS");
            // @formatter:on

            // Verify that the Zookeeper client was cleaned up and null
            assertThat(getField(limiter, CuratorFramework.class, "zkClient")).isNull();
        }

        @DisplayName("does not affect the zookeeper client after a second successful call")
        @Test
        void zookeeperClientIsSameAfterSuccess() throws NoSuchFieldException, IllegalAccessException {
            assertFieldIsSameAfterSecondSuccessfulSetup(CuratorFramework.class, "zkClient");
        }

        @DisplayName("does not affect the zookeeper client after a second failing call")
        @Test
        void zookeeperClientIsSameAfterFailure() throws NoSuchFieldException, IllegalAccessException {
            assertFieldIsSameAfterSecondFailingSetup(CuratorFramework.class, "zkClient");
        }

        @DisplayName("does not affect the heartbeat cache after a second successful call")
        @Test
        void heartbeatCacheIsSameAfterSuccess() throws NoSuchFieldException, IllegalAccessException {
            assertFieldIsSameAfterSecondSuccessfulSetup(QueryHeartbeatCache.class, "heartbeatCache");
        }

        @DisplayName("does not affect the heartbeat cache after a second failing call")
        @Test
        void heartbeatCacheIsSameAfterFailure() throws NoSuchFieldException, IllegalAccessException {
            assertFieldIsSameAfterSecondFailingSetup(QueryHeartbeatCache.class, "heartbeatCache");
        }

        @DisplayName("does not affect the active query tracker after a second successful call")
        @Test
        void activeQueryTrackerIsSameAfterSuccess() throws NoSuchFieldException, IllegalAccessException {
            assertFieldIsSameAfterSecondSuccessfulSetup(ActiveQueryTracker.class, "activeQueryTracker");
        }

        @DisplayName("does not affect the active query tracker after a second failing call")
        @Test
        void activeQueryTrackerIsSameAfterFailure() throws NoSuchFieldException, IllegalAccessException {
            assertFieldIsSameAfterSecondFailingSetup(ActiveQueryTracker.class, "activeQueryTracker");
        }

        private <T> void assertFieldIsSameAfterSecondSuccessfulSetup(Class<T> clazz, String fieldName) throws NoSuchFieldException, IllegalAccessException {
            assertFieldIsSameBetweenCallsToSetup(clazz, fieldName, queryLimiter -> {
                QueryLimiterImplConfiguration config = createConfig();
                config.getLimitConfiguration().setDefaultUserQueryLimit(500);
                queryLimiter.setConfiguration(config);
                queryLimiter.setup();
            });
        }

        private <T> void assertFieldIsSameAfterSecondFailingSetup(Class<T> clazz, String fieldName) throws NoSuchFieldException, IllegalAccessException {
            assertFieldIsSameBetweenCallsToSetup(clazz, fieldName, queryLimiter -> {
                QueryLimiterImplConfiguration config = createConfig();
                config.setLimitConfiguration(null);
                queryLimiter.setConfiguration(config);
                assertThatThrownBy(queryLimiter::setup).isInstanceOf(RuntimeException.class);
            });
        }

        private <T> void assertFieldIsSameBetweenCallsToSetup(Class<T> clazz, String fieldName, Consumer<QueryLimiterImpl> secondSetupCall)
                        throws NoSuchFieldException, IllegalAccessException {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());

            // The field should be null before setup() is called.
            assertThat(getField(limiter, clazz, fieldName)).isNull();

            // The field should not be null after setup() is called for the first time.
            limiter.setup();
            T fieldAfterFirstSetup = getField(limiter, clazz, fieldName);
            assertThat(fieldAfterFirstSetup).isNotNull();

            // Call setup() again.
            secondSetupCall.accept(limiter);

            // The field should still be the same instance after setup() is called the second time.
            T fieldAfterSecondSetup = getField(limiter, clazz, fieldName);
            assertThat(fieldAfterSecondSetup).isSameAs(fieldAfterFirstSetup);
        }

    }

    @DisplayName("Method close()")
    @Nested
    class CloseBasicFunctionality {

        @DisplayName("nullifies all internal resources")
        @Test
        void nullifiesAllProperties() throws NoSuchFieldException, IllegalAccessException {
            // Create a working limiter.
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());
            limiter.setup();
            assertThat(limiter.isEnabled()).isTrue();

            // All internal resources should be initialized.
            assertThat(getField(limiter, QueryLogicGroupLimitProvider.class, "queryLogicGroupLimitProvider")).isNotNull();
            assertThat(getField(limiter, UserLimitProvider.class, "userLimitProvider")).isNotNull();
            assertThat(getField(limiter, SystemLimitProvider.class, "systemLimitProvider")).isNotNull();
            assertThat(getField(limiter, CuratorFramework.class, "zkClient")).isNotNull();
            assertThat(getField(limiter, QueryHeartbeatCache.class, "heartbeatCache")).isNotNull();
            assertThat(getField(limiter, ActiveQueryTracker.class, "activeQueryTracker")).isNotNull();

            // Shutdown the limiter.
            limiter.close();

            // All internal resources should be null.
            assertThat(getField(limiter, QueryLogicGroupLimitProvider.class, "queryLogicGroupLimitProvider")).isNull();
            assertThat(getField(limiter, UserLimitProvider.class, "userLimitProvider")).isNull();
            assertThat(getField(limiter, SystemLimitProvider.class, "systemLimitProvider")).isNull();
            assertThat(getField(limiter, CuratorFramework.class, "zkClient")).isNull();
            assertThat(getField(limiter, QueryHeartbeatCache.class, "heartbeatCache")).isNull();
            assertThat(getField(limiter, ActiveQueryTracker.class, "activeQueryTracker")).isNull();
        }

        @DisplayName("transitions to a CLOSED state")
        @Test
        void resultsInClosedState() {
            // Create a working limiter.
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());
            limiter.setup();
            assertThat(limiter.isEnabled()).isTrue();

            limiter.close();

            assertThat(limiter.getState()).isEqualTo(QueryLimiterImpl.State.CLOSED);
            assertThat(limiter.isEnabled()).isFalse();
        }

        @DisplayName("call be called multiple times without error")
        @Test
        void callBeCalledMultipleTimes() {
            // Create a working limiter.
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.setConfiguration(createConfig());
            limiter.setup();
            assertThat(limiter.isEnabled()).isTrue();

            limiter.close();
            limiter.close();
        }
    }

    @DisplayName("Method checkLimits() identifies when")
    @Nested
    class CheckLimitsBasicFunctionality {

        private static final String userA = "cn=testuserA, c=us";
        private static final String userB = "cn=testuserB, c=us";
        private static final String system1 = "SYSTEM-01";
        private static final String system2 = "SYSTEM-02";
        private static final String tldQueryLogic = "TLDQueryLogic";
        private static final String eventQueryLogic = "EventQueryLogic";

        private final Map<String,QueryLimiterImpl> systemToLimiter = new HashMap<>();
        private QueryLimitConfiguration limitConfig;

        @AfterEach
        void tearDown() {
            systemToLimiter.clear();
            limitConfig = null;
        }

        @DisplayName("default max user query limit is met")
        @Test
        void defaultUserQueryLimitMet() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(100);
            config.setDefaultUserQueryLimit(5);
            givenConfig(config);

            startQueries(1, userA, system1, tldQueryLogic);
            startQueries(1, userA, system1, eventQueryLogic);
            startQueries(1, userA, system2, tldQueryLogic);
            startQueries(1, userA, system2, eventQueryLogic);

            // Verify that four active queries for the same user do not meet a limit.
            assertLimitNotMet(userA, system1, tldQueryLogic);

            startQueries(1, userA, system2, tldQueryLogic);

            // Verify that after we've created five queries for the same user (across different servers and query logics), we have met a limit.
            assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 5 running queries");
        }

        @DisplayName("default system query limit is met")
        @Test
        void defaultSystemQueryLimitMet() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(8);
            config.setDefaultUserQueryLimit(5);
            givenConfig(config);

            startQueries(2, userA, system1, tldQueryLogic);
            startQueries(2, userA, system1, eventQueryLogic);
            startQueries(3, userB, system1, tldQueryLogic);

            // Verify that seven active queries for the same system do not meet a limit.
            assertLimitNotMet(userA, system1, tldQueryLogic);

            startQueries(1, userB, system1, tldQueryLogic);

            // Verify that after we've created seven queries for the same system (across different users and query logics), we have met a limit.
            assertLimitMet(userA, system1, tldQueryLogic, "System 'SYSTEM-01' has reached limit of 8 running queries");
        }

        @DisplayName("default query logic group limit is met")
        @Test
        void defaultQueryLogicGroupLimit() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(100);
            config.setDefaultUserQueryLimit(10);

            // Set a default limit of 3 TLDQueryLogic queries per user.
            QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
            groupLimitConfig.setGroupName("TLD");
            groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
            groupLimitConfig.setQueryLimit(3);
            config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));
            givenConfig(config);

            startQueries(1, userA, system1, tldQueryLogic);
            startQueries(1, userA, system2, tldQueryLogic);

            // Verify that two active TLDQueryLogics queries for userA do not meet a limit.
            assertLimitNotMet(userA, system1, tldQueryLogic);

            startQueries(1, userA, system2, tldQueryLogic);

            // Verify that after we've created three queries for userA (across different servers), we have met a limit.
            assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 3 running queries for query logic group 'TLD'");

            // Assert that the user could still create queries for the EventQueryLogic.
            assertLimitNotMet(userA, system1, eventQueryLogic);
        }

        @DisplayName("custom user query limit is met")
        @Test
        void customUserQueryLimitMet() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(100);
            config.setDefaultUserQueryLimit(5);

            // Set a custom limit of 8 queries for userA.
            UserLimitConfiguration userConfig = new UserLimitConfiguration();
            userConfig.setUserDn(userA);
            userConfig.setQueryLimit(8);
            config.setUserConfigs(List.of(userConfig));
            givenConfig(config);

            startQueries(3, userA, system1, tldQueryLogic);
            startQueries(4, userA, system2, eventQueryLogic);

            // Verify that three active TLDQueryLogics queries for userA do not meet a limit.
            assertLimitNotMet(userA, system1, tldQueryLogic);

            startQueries(1, userA, system2, tldQueryLogic);

            // Verify that after we've created four TLDQueryLogic queries for userA (across different servers), we have met a limit.
            assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 8 running queries");

            startQueries(5, userB, system1, eventQueryLogic);

            // Verify that userB, who has no custom limit, may not have more than 5 queries.
            assertLimitMet(userB, system1, tldQueryLogic, "User 'cn=testuserb, c=us' has reached limit of 5 running queries");
        }

        @DisplayName("custom user query logic group limit is met")
        @Test
        void customUserQueryLogicGroupLimitMet() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(100);
            config.setDefaultUserQueryLimit(10);

            // Set a default limit of 5 TLDQueryLogic queries per user.
            QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
            groupLimitConfig.setGroupName("TLD");
            groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
            groupLimitConfig.setQueryLimit(5);
            config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));

            // Set a custom limit of 3 TLDQueryLogic queries for userA.
            UserLimitConfiguration userConfig = new UserLimitConfiguration();
            userConfig.setUserDn(userA);
            userConfig.setQueryLogicGroupLimits(Map.of("TLD", 3));
            config.setUserConfigs(List.of(userConfig));
            givenConfig(config);

            startQueries(1, userA, system1, tldQueryLogic);
            startQueries(1, userA, system2, tldQueryLogic);

            // Verify that two active TLDQueryLogic queries for userA do not meet a limit.
            assertLimitNotMet(userA, system1, tldQueryLogic);

            startQueries(1, userA, system2, tldQueryLogic);

            // Verify that after we've created three TLDQueryLogic queries for userA (across different servers), we have met a limit.
            assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 3 running queries for query logic group 'TLD'");

            // Verify that userB, who has no custom limit, may have up to 5 TLDQueryLogic queries.
            startQueries(4, userB, system1, tldQueryLogic);

            assertLimitNotMet(userB, system1, tldQueryLogic);

            startQueries(1, userB, system1, tldQueryLogic);

            assertLimitMet(userB, system1, tldQueryLogic, "User 'cn=testuserb, c=us' has reached limit of 5 running queries for query logic group 'TLD'");
        }

        @DisplayName("custom server query limit is met")
        @Test
        void customServerQueryLimit() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(100);
            config.setDefaultUserQueryLimit(25);

            // Set a limit of 10 queries for system2.
            SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
            systemConfig.setSystemPattern(system2);
            systemConfig.setQueryLimit(10);
            config.setSystemConfigs(List.of(systemConfig));
            givenConfig(config);

            startQueries(4, userA, system2, tldQueryLogic);
            startQueries(5, userB, system2, eventQueryLogic);

            // Verify that the nine active queries on system2 do not meet a limit.
            assertLimitNotMet(userA, system2, tldQueryLogic);

            startQueries(1, userA, system2, eventQueryLogic);

            // Verify that after we've created ten queries for system2 (across different users), we have met a limit.
            assertLimitMet(userA, system2, tldQueryLogic, "System 'SYSTEM-02' has reached limit of 10 running queries");

            // Verify that system1 is not bound to the same limit as system2.
            startQueries(15, userB, system1, eventQueryLogic);
            assertLimitNotMet(userB, system1, tldQueryLogic);
        }

        @DisplayName("custom server query logic group limit is met")
        @Test
        void customServerQueryLogicGroupLimit() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(100);
            config.setDefaultUserQueryLimit(50);

            // Set a default limit of 20 TLDQueryLogic queries per user.
            QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
            groupLimitConfig.setGroupName("TLD");
            groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
            groupLimitConfig.setQueryLimit(20);
            config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));

            // Set a limit of 10 queries for system2.
            SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
            systemConfig.setSystemPattern(system2);
            systemConfig.setQueryLogicGroupLimits(Map.of("TLD", 10));
            config.setSystemConfigs(List.of(systemConfig));
            givenConfig(config);

            startQueries(4, userA, system2, tldQueryLogic);
            startQueries(5, userB, system2, tldQueryLogic);

            // Verify that nine active TLDQueryLogic queries for system2 do not meet a limit.
            assertLimitNotMet(userA, system2, tldQueryLogic);

            startQueries(1, userA, system2, tldQueryLogic);

            // Verify that after we've created ten TLDQueryLogic queries on system2 (across different users), we have met a limit.
            assertLimitMet(userA, system2, tldQueryLogic, "System 'SYSTEM-02' has reached limit of 10 running queries for query logic group 'TLD'");

            // Verify that system1, which has no custom limit, will not enforce any system-wide limits for TLDQueryLogic queries. TLDQueryLogic query limits
            // outside
            // system2 only apply to individual users

            // Create enough queries for userA and userB to reach their individual limits for TLDQueryLogic queries.
            startQueries(15, userA, system1, tldQueryLogic);
            startQueries(15, userB, system1, tldQueryLogic);

            assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 20 running queries for query logic group 'TLD'");
            assertLimitMet(userB, system1, tldQueryLogic, "User 'cn=testuserb, c=us' has reached limit of 20 running queries for query logic group 'TLD'");
        }

        /**
         * Verify that even if a system does not count against the user query limit, the system meeting a custom query limit results in a met limit response.
         */
        @DisplayName("custom server (that does not count against user limits) query limit is met")
        @Test
        void customServerQueryLimitWhereServerDoesNotCountAgainstUserLimit() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(100);
            config.setDefaultUserQueryLimit(25);

            // Set a limit of 10 queries for system2.
            SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
            systemConfig.setSystemPattern(system2);
            systemConfig.setQueryLimit(10);
            systemConfig.setCountsAgainstUserLimit(false);
            config.setSystemConfigs(List.of(systemConfig));
            givenConfig(config);

            startQueries(4, userA, system2, tldQueryLogic);
            startQueries(5, userB, system2, eventQueryLogic);

            // Verify that the nine active queries on system2 do not meet a limit.
            assertLimitNotMet(userA, system2, tldQueryLogic);

            startQueries(1, userA, system2, eventQueryLogic);

            // Verify that after we've created ten queries for system2 (across different users), we have met a limit.
            assertLimitMet(userA, system2, tldQueryLogic, "System 'SYSTEM-02' has reached limit of 10 running queries");

            // Verify that system1 is not bound to the same limit as system2.
            startQueries(15, userB, system1, eventQueryLogic);
            assertLimitNotMet(userB, system1, tldQueryLogic);
        }

        /**
         * Verify that even if a system does not count against the user query limit, the system meeting a custom query logic group limit results in a met limit
         * response.
         */
        @DisplayName("custom server (that does not count against user limits) query logic group limit is met")
        @Test
        void customServerQueryLogicGroupLimitWhereServerDoesNotCountAgainstUserLimit() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(100);
            config.setDefaultUserQueryLimit(50);

            // Set a default limit of 20 TLDQueryLogic queries per user.
            QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
            groupLimitConfig.setGroupName("TLD");
            groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
            groupLimitConfig.setQueryLimit(20);
            config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));

            // Set a limit of 10 TLDQueryLogic queries for system2.
            SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
            systemConfig.setSystemPattern(system2);
            systemConfig.setCountsAgainstUserLimit(false);
            systemConfig.setQueryLogicGroupLimits(Map.of("TLD", 10));
            config.setSystemConfigs(List.of(systemConfig));
            givenConfig(config);

            startQueries(4, userA, system2, tldQueryLogic);
            startQueries(5, userB, system2, tldQueryLogic);

            // Verify that nine active TLDQueryLogic queries for system2 do not meet a limit.
            assertLimitNotMet(userA, system2, tldQueryLogic);

            startQueries(1, userA, system2, tldQueryLogic);

            // Verify that after we've created ten TLDQueryLogic queries on system2 (across different users), we have met a limit.
            assertLimitMet(userA, system2, tldQueryLogic, "System 'SYSTEM-02' has reached limit of 10 running queries for query logic group 'TLD'");

            // Verify that system1, which has no custom limit, will not enforce any system-wide limits for TLDQueryLogic queries. TLDQueryLogic query limits
            // outside
            // system2 only apply to individual users, and system2 queries do not count against the user limit.

            // Create 19 queries on system1. We should still be under the TLDQueryLogic limit since all previous queries do not count against the limit.
            startQueries(19, userA, system1, tldQueryLogic);
            startQueries(19, userB, system1, tldQueryLogic);

            assertLimitNotMet(userA, system1, tldQueryLogic);
            assertLimitNotMet(userB, system1, tldQueryLogic);

            // Create the 20th TLDQueryLogic query for each user on system1. This should meet the limit.
            startQueries(1, userA, system1, tldQueryLogic);
            startQueries(1, userB, system1, tldQueryLogic);

            assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 20 running queries for query logic group 'TLD'");
            assertLimitMet(userB, system1, tldQueryLogic, "User 'cn=testuserb, c=us' has reached limit of 20 running queries for query logic group 'TLD'");
        }

        /**
         * Verify that if a system does not count against the user query limit, that it does not count towards any limits other than:
         * <ul>
         * <li>The query limit specified for the system (custom if specified, or default otherwise).</li>
         * <li>Any custom query logic group limits specified for the limit.</li>
         * </ul>
         */
        @DisplayName("server does not count towards user limits")
        @Test
        void serverThatDoesNotCountAgainstUserLimit() throws Exception {
            QueryLimitConfiguration config = new QueryLimitConfiguration();
            config.setDefaultSystemQueryLimit(100);
            config.setDefaultUserQueryLimit(20);

            // Set a default limit of 20 TLDQueryLogic queries per user.
            QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
            groupLimitConfig.setGroupName("TLD");
            groupLimitConfig.setQueryLogicPattern("TLDQueryLogic");
            groupLimitConfig.setQueryLimit(10);
            config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));

            // Establish that system2 does not count against the user query limit.
            SystemLimitConfiguration systemConfig = new SystemLimitConfiguration();
            systemConfig.setSystemPattern(system2);
            systemConfig.setCountsAgainstUserLimit(false);
            config.setSystemConfigs(List.of(systemConfig));
            givenConfig(config);

            // Verify that after creating 25 TLDQueryLogic queries on system2, that we do not meet any limits since system2 does not count towards user limits.
            startQueries(25, userA, system2, tldQueryLogic);

            assertLimitNotMet(userA, system1, tldQueryLogic);
            assertLimitNotMet(userA, system2, tldQueryLogic);

            // Verify that after creating 10 TLDQueryLogic queries on system1 for userA, we meet the TLDQueryLogicLimit.
            startQueries(10, userA, system1, tldQueryLogic);

            assertLimitMet(userA, system1, tldQueryLogic, "User 'cn=testusera, c=us' has reached limit of 10 running queries for query logic group 'TLD'");
        }

        private QueryLimiterImpl getLimiter(String system) {
            if (systemToLimiter.containsKey(system)) {
                return systemToLimiter.get(system);
            } else {
                QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
                config.setZkClientBuilder(new ZkClientBuilder().withConnectString(server.getConnectString()));
                config.setLimitConfiguration(limitConfig);

                QueryLimiterImpl limiter = new QueryLimiterImpl();
                limiter.setConfiguration(config);
                limiter.setup();
                systemToLimiter.put(system, limiter);
                return limiter;
            }
        }

        private List<String> startQueries(int numQueries, String userDn, String system, String queryLogic) throws Exception {
            List<String> queryIds = new ArrayList<>(numQueries);
            QueryLimiterImpl limiter = getLimiter(system);
            for (int i = 0; i < numQueries; i++) {
                String queryId = UUID.randomUUID().toString();
                limiter.markActive(queryId, userDn, system, queryLogic);
                queryIds.add(queryId);
            }
            return queryIds;
        }

        private void assertLimitNotMet(String userDn, String system, String queryLogic) throws Exception {
            QueryLimiterImpl limiter = getLimiter(system);
            QueryLimiterResponse response = limiter.checkLimits(userDn, system, queryLogic);
            assertThat(response.getMessage()).isNull();
            assertThat(response.metLimit()).isFalse();
        }

        private void assertLimitMet(String userDn, String system, String queryLogic, String message) throws Exception {
            QueryLimiterImpl limiter = getLimiter(system);
            QueryLimiterResponse response = limiter.checkLimits(userDn, system, queryLogic);
            assertThat(response.getMessage()).isEqualTo(message);
            assertThat(response.metLimit()).isTrue();
        }

        private void givenConfig(QueryLimitConfiguration config) {
            this.limitConfig = config;
        }
    }

    private <T> T getField(Object source, Class<T> clazz, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field field = source.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return clazz.cast(field.get(source));
    }

    private QueryLimiterImplConfiguration createConfig() {
        QueryLimitConfiguration limitConfig = new QueryLimitConfiguration();
        limitConfig.setDefaultUserQueryLimit(100);
        limitConfig.setInternalCacheMaxSize(200);

        QueryLimiterImplConfiguration config = new QueryLimiterImplConfiguration();
        config.setLimitConfiguration(limitConfig);
        config.setZkClientBuilder(new ZkClientBuilder().withConnectString(server.getConnectString()));

        return config;
    }
}
