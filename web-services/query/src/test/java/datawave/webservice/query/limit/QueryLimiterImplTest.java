package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.Field;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test cases for testing the functionality of {@link QueryLimiterImpl}.
 */
@ExtendWith(MockitoExtension.class)
class QueryLimiterImplTest {
    
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
    
    @InjectMocks
    private QueryLimiterImpl limiter;
    
    @Mock
    private ActiveQueryTracker activeQueryTracker;
    
    @Mock
    private UserLimitProvider userLimitProvider;
    
    @Mock
    private QueryLogicGroupLimitProvider groupLimitProvider;
    
    @Mock
    private SystemLimitProvider systemLimitProvider;
    
    @Mock
    private QueryHeartbeatCache heartbeatCache;
    
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
    
    @DisplayName("Method checkLimits() throws an exception when")
    @Nested
    class CheckLimitExpectedExceptions {
        
        @DisplayName("the user dn is null")
        @Test
        void nullUserDn() {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            assertThatThrownBy(() -> limiter.checkLimits(null, "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("userDn cannot be null or blank");
        }
        
        @DisplayName("the user dn is blank")
        @Test
        void blankUserDn() {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            assertThatThrownBy(() -> limiter.checkLimits("  ", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("userDn cannot be null or blank");
        }
        
        @DisplayName("the query logic is null")
        @Test
        void nullQueryLogic() {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            assertThatThrownBy(() -> limiter.checkLimits("userA", "SYSTEM-01", null)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("queryLogic cannot be null or blank");
        }
        
        @DisplayName("the query logic is blank")
        @Test
        void blankQueryLogic() {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            assertThatThrownBy(() -> limiter.checkLimits("userA", "SYSTEM-01", "   ")).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("queryLogic cannot be null or blank");
        }
        
        @DisplayName("the query limiter has the state UNINITIALIZED")
        @Test
        void uninitializedState() {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            assertThatThrownBy(() -> limiter.checkLimits("userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Checking limits not allowed while limiter is in state UNINITIALIZED");
        }
        
        @DisplayName("the query limiter has the state CLOSED")
        @Test
        void closedState() {
            QueryLimiterImpl limiter = new QueryLimiterImpl();
            limiter.close();
            assertThatThrownBy(() -> limiter.checkLimits("userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Checking limits not allowed while limiter is in state CLOSED");
        }
    }
    
    @DisplayName("Method checkLimits() identifies when")
    @Nested
    class CheckLimitsBasicFunctionality {
    
    
    
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
