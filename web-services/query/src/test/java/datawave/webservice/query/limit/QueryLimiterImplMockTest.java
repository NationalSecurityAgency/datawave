package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * General-purpose tests for {@link QueryLimiterImpl} that only require mocked interactions.
 */
@ExtendWith(MockitoExtension.class)
class QueryLimiterImplMockTest {

    @InjectMocks
    private QueryLimiterImpl limiter;

    @Mock
    private QueryLimiterImplConfiguration limiterConfig;

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

    /**
     * Tests for {@link QueryLimiterImpl#checkLimits(String, String, String)}.
     */
    @DisplayName("Method checkLimits()")
    @Nested
    class CheckLimitsTests {

        @DisplayName("throws an exception when")
        @Nested
        class UnhappyPaths {

            @DisplayName("the user dn is null")
            @Test
            void nullUserDn() {
                assertThatThrownBy(() -> limiter.checkLimits(null, "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("userDn cannot be null or blank");
            }

            @DisplayName("the user dn is blank")
            @Test
            void blankUserDn() {
                assertThatThrownBy(() -> limiter.checkLimits("  ", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("userDn cannot be null or blank");
            }

            @DisplayName("the query logic is null")
            @Test
            void nullQueryLogic() {
                assertThatThrownBy(() -> limiter.checkLimits("userA", "SYSTEM-01", null)).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("queryLogic cannot be null or blank");
            }

            @DisplayName("the query logic is blank")
            @Test
            void blankQueryLogic() {
                assertThatThrownBy(() -> limiter.checkLimits("userA", "SYSTEM-01", "   ")).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("queryLogic cannot be null or blank");
            }

            @ParameterizedTest(name = "the query limiter has the state {0}")
            @EnumSource(value = QueryLimiterImpl.State.class, mode = EnumSource.Mode.EXCLUDE, names = {"READY"})
            void invalidStates(QueryLimiterImpl.State state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(state);
                // @formatter:off
                assertThatThrownBy(() -> limiter.checkLimits("userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(QueryLimitException.class)
                                .hasMessage("Error while checking limits for userDn: usera, system: SYSTEM-01, queryLogic: EventQueryLogic")
                                .hasRootCauseInstanceOf(IllegalStateException.class)
                                .hasRootCauseMessage("Query limiter is not in a READY state");
                // @formatter:on
            }
        }

        @DisplayName("correctly")
        @Nested
        class BasicFunctionality {

            @DisplayName("does not enforce limits when the query limiter mode is ")
            @ParameterizedTest(name = "{0}")
            @EnumSource(value = QueryLimiterMode.class, mode = EnumSource.Mode.EXCLUDE, names = {"ENFORCE"})
            void limitsNotEnforced(QueryLimiterMode mode) throws Exception {
                setLimiterState(QueryLimiterImpl.State.READY);
                when(limiterConfig.getMode()).thenReturn(mode);

                QueryLimiterResponse response = limiter.checkLimits("userA", "SYSTEM-01", "EventQueryLogic");

                assertThat(response.metLimit()).isFalse();
                verifyNoInteractions(activeQueryTracker);
            }
        }
    }

    /**
     * Tests for {@link QueryLimiterImpl#markActive(String, String, String, String)}.
     */
    @DisplayName("Method markActive()")
    @Nested
    class MarkActiveTests {

        @DisplayName("Throws an exception when")
        @Nested
        class UnhappyPaths {

            @DisplayName("the query id is null")
            @Test
            void nullQueryId() {
                assertThatThrownBy(() -> limiter.markActive(null, "userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("queryId cannot be null or blank");
            }

            @DisplayName("the query id is blank")
            @Test
            void blankQueryId() {
                assertThatThrownBy(() -> limiter.markActive("  ", "userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("queryId cannot be null or blank");
            }

            @DisplayName("the user dn is null")
            @Test
            void nullUserDn() {
                assertThatThrownBy(() -> limiter.markActive("queryId", null, "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("userDn cannot be null or blank");
            }

            @DisplayName("the user dn is blank")
            @Test
            void blankUserDn() {
                assertThatThrownBy(() -> limiter.markActive("queryId", "  ", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("userDn cannot be null or blank");
            }

            @DisplayName("the query logic is null")
            @Test
            void nullQueryLogic() {
                assertThatThrownBy(() -> limiter.markActive("queryId", "userA", "SYSTEM-01", null)).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("queryLogic cannot be null or blank");
            }

            @DisplayName("the query logic is blank")
            @Test
            void blankQueryLogic() {
                assertThatThrownBy(() -> limiter.markActive("queryId", "userA", "SYSTEM-01", "   ")).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("queryLogic cannot be null or blank");
            }

            @DisplayName("the query limiter has the state")
            @ParameterizedTest(name = "{0}")
            @EnumSource(value = QueryLimiterImpl.State.class, mode = EnumSource.Mode.EXCLUDE, names = {"READY"})
            void invalidStates(QueryLimiterImpl.State state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(state);
                // @formatter:off
                assertThatThrownBy(() -> limiter.markActive("1234", "userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(QueryLimitException.class)
                                .hasMessage("Error while marking query 1234 active")
                                .hasRootCauseInstanceOf(IllegalStateException.class)
                                .hasRootCauseMessage("Query limiter is not in a READY state");
                // @formatter:on
            }
        }

        @DisplayName("correctly")
        @Nested
        class BasicFunctionality {

            @DisplayName("normalizes the user DN, system, and query logic")
            @Test
            void normalizesArgs() throws Exception {
                setLimiterState(QueryLimiterImpl.State.READY);
                when(limiterConfig.getMode()).thenReturn(QueryLimiterMode.ENFORCE);

                limiter.markActive(" queryId ", " CN=UserA ", " Artemis-01 ", " TLDQueryLogic ");
                verify(activeQueryTracker).trackQuery(eq("queryId"), eq("cn=usera"), eq("Artemis-01"), eq("TLDQueryLogic"), anyBoolean());
            }

            @DisplayName("uses EMPTY_SYSTEM_FROM if the system is null")
            @Test
            void usesDefaultSystemIfNullSystem() throws Exception {
                setLimiterState(QueryLimiterImpl.State.READY);
                when(limiterConfig.getMode()).thenReturn(QueryLimiterMode.ENFORCE);

                limiter.markActive("queryId", " CN=UserA ", null, " TLDQueryLogic ");

                verify(activeQueryTracker).trackQuery(eq("queryId"), eq("cn=usera"), eq("EMPTY_SYSTEM_FROM"), eq("TLDQueryLogic"), anyBoolean());
            }

            @DisplayName("uses EMPTY_SYSTEM_FROM if the system is blank")
            @Test
            void usesDefaultSystemIfBlankSystem() throws Exception {
                setLimiterState(QueryLimiterImpl.State.READY);
                when(limiterConfig.getMode()).thenReturn(QueryLimiterMode.ENFORCE);

                limiter.markActive("queryId", " CN=UserA ", "  ", " TLDQueryLogic ");

                verify(activeQueryTracker).trackQuery(eq("queryId"), eq("cn=usera"), eq("EMPTY_SYSTEM_FROM"), eq("TLDQueryLogic"), anyBoolean());
            }

            @DisplayName("does not track the query when the query limiter mode is")
            @ParameterizedTest(name = "{0}")
            @EnumSource(value = QueryLimiterMode.class, mode = EnumSource.Mode.INCLUDE, names = {"DISABLED"})
            void trackingDisabled(QueryLimiterMode mode) throws Exception {
                setLimiterState(QueryLimiterImpl.State.READY);
                when(limiterConfig.getMode()).thenReturn(mode);

                limiter.markActive("1234", "userA", "SYSTEM-01", "EventQueryLogic");

                verifyNoInteractions(activeQueryTracker);
            }

            @DisplayName("tracks the query when the query limiter mode is")
            @ParameterizedTest(name = "{0}")
            @EnumSource(value = QueryLimiterMode.class, mode = EnumSource.Mode.EXCLUDE, names = {"DISABLED"})
            void trackingEnabled(QueryLimiterMode mode) throws Exception {
                setLimiterState(QueryLimiterImpl.State.READY);
                when(limiterConfig.getMode()).thenReturn(mode);

                limiter.markActive("1234", "cn=userA", "SYSTEM-01", "EventQueryLogic");

                verify(activeQueryTracker).trackQuery(eq("1234"), eq("cn=usera"), eq("SYSTEM-01"), eq("EventQueryLogic"), anyBoolean());
            }
        }
    }

    /**
     * Tests for {@link QueryLimiterImpl#getActiveQueries()}.
     */
    @DisplayName("Method getActiveQueries()")
    @Nested
    class GetActiveQueriesTests {

        @DisplayName("throws an exception when")
        @Nested
        class UnhappyPaths {

            @DisplayName("the query limiter has the state")
            @ParameterizedTest(name = "{0}")
            @EnumSource(value = QueryLimiterImpl.State.class, mode = EnumSource.Mode.EXCLUDE, names = {"READY"})
            void invalidStates(QueryLimiterImpl.State state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(state);
                // @formatter:off
                assertThatThrownBy(() -> limiter.getActiveQueries()).isInstanceOf(QueryLimitException.class)
                                .hasMessage("Error while fetching active queries")
                                .hasRootCauseInstanceOf(IllegalStateException.class)
                                .hasRootCauseMessage("Query limiter is not in a READY state");
                // @formatter:on
            }
        }

        @DisplayName("correctly")
        @Nested
        class BasicFunctionality {

            @DisplayName("returns queries from the heartbeat cache in state READY")
            @Test
            void readyState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(QueryLimiterImpl.State.READY);
                limiter.getActiveQueries();
                verify(heartbeatCache).getQueryIds();
            }
        }
    }

    /**
     * Tests for {@link QueryLimiterImpl#markInactive(Collection)}.
     */
    @DisplayName("Method markInactive(Collection<String>)")
    @Nested
    class MarkMultipleInactiveTests {

        @DisplayName("throws an exception when")
        @Nested
        class UnhappyPaths {

            @DisplayName("the query id collection is null")
            @Test
            void nullQueryIds() {
                assertThatThrownBy(() -> limiter.markInactive((Collection<String>) null)).isInstanceOf(NullPointerException.class)
                                .hasMessage("queryIds cannot be null");
            }

            @DisplayName("the query id collection contains a null queryId")
            @Test
            void nullQueryId() {
                Set<String> set = new HashSet<>();
                set.add(null);
                assertThatThrownBy(() -> limiter.markInactive(set)).isInstanceOf(IllegalArgumentException.class).hasMessage("queryId cannot be null or blank");
            }

            @DisplayName("the query id collection contains a blank query id")
            @Test
            void blankQueryId() {
                assertThatThrownBy(() -> limiter.markInactive(Set.of("  "))).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("queryId cannot be null or blank");
            }

            @DisplayName("the query limiter has the state")
            @ParameterizedTest(name = "{0}")
            @EnumSource(value = QueryLimiterImpl.State.class, mode = EnumSource.Mode.EXCLUDE, names = {"READY"})
            void invalidStates(QueryLimiterImpl.State state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(state);
                // @formatter:off
                assertThatThrownBy(() -> limiter.markInactive(Set.of("1234"))).isInstanceOf(QueryLimitException.class)
                                .hasMessage("Error while marking queries inactive: [1234]")
                                .hasRootCauseInstanceOf(IllegalStateException.class)
                                .hasRootCauseMessage("Query limiter is not in a READY state");
                // @formatter:on
            }
        }

        @DisplayName("correctly")
        @Nested
        class BasicFunctionality {

            @DisplayName("normalizes the query ids")
            @Test
            void normalizesQueryIds() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(QueryLimiterImpl.State.READY);
                Set<String> queryIds = Set.of("  queryId1  ");

                limiter.markInactive(queryIds);

                verify(heartbeatCache).stopAndRemove(eq(Set.of("queryId1")));
            }

            @DisplayName("stops and removes queries in the heartbeat cache in state READY")
            @Test
            void readyState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(QueryLimiterImpl.State.READY);
                Set<String> queryIds = Set.of("queryId1");

                limiter.markInactive(queryIds);

                verify(heartbeatCache).stopAndRemove(eq(queryIds));
            }

            @DisplayName("does not care when the query limiter mode is")
            @ParameterizedTest(name = "{0}")
            @EnumSource(value = QueryLimiterMode.class, mode = EnumSource.Mode.MATCH_ALL)
            void modeAgnostic(QueryLimiterMode mode) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(QueryLimiterImpl.State.READY);
                lenient().when(limiterConfig.getMode()).thenReturn(mode);

                Set<String> queryIds = Set.of("queryId1");
                limiter.markInactive(queryIds);

                verify(heartbeatCache).stopAndRemove(eq(queryIds));
            }
        }
    }

    /**
     * Tests for {@link QueryLimiterImpl#markInactive(String)}.
     */
    @DisplayName("Method markInactive(String)")
    @Nested
    class MarkSingleInactiveTests {

        @DisplayName("throws an exception when")
        @Nested
        class UnhappyPaths {

            @DisplayName("the queryId is null")
            @Test
            void nullQueryId() {
                assertThatThrownBy(() -> limiter.markInactive((String) null)).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("queryId cannot be null or blank");
            }

            @DisplayName("the queryId is blank")
            @Test
            void blankQueryIds() {
                assertThatThrownBy(() -> limiter.markInactive("   ")).isInstanceOf(IllegalArgumentException.class)
                                .hasMessage("queryId cannot be null or blank");
            }

            @DisplayName("the query limiter has the state")
            @ParameterizedTest(name = "{0}")
            @EnumSource(value = QueryLimiterImpl.State.class, mode = EnumSource.Mode.EXCLUDE, names = {"READY"})
            void invalidStates(QueryLimiterImpl.State state) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(state);
                // @formatter:off
                assertThatThrownBy(() -> limiter.markInactive("1234")).isInstanceOf(QueryLimitException.class)
                                .hasMessage("Error while marking query 1234 inactive")
                                .hasRootCauseInstanceOf(IllegalStateException.class)
                                .hasRootCauseMessage("Query limiter is not in a READY state");
                // @formatter:on
            }
        }

        @DisplayName("correctly")
        @Nested
        class BasicFunctionality {

            @DisplayName("stops and removes the query in the heartbeat cache in state READY")
            @Test
            void readyState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(QueryLimiterImpl.State.READY);
                limiter.markInactive("1234");
                verify(heartbeatCache).stopAndRemove(eq("1234"));
            }

            @DisplayName("does not care when the query limiter mode is")
            @ParameterizedTest(name = "{0}")
            @EnumSource(value = QueryLimiterMode.class, mode = EnumSource.Mode.MATCH_ALL)
            void modeAgnostic(QueryLimiterMode mode) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
                setLimiterState(QueryLimiterImpl.State.READY);
                lenient().when(limiterConfig.getMode()).thenReturn(mode);

                limiter.markInactive("1234");

                verify(heartbeatCache).stopAndRemove(eq("1234"));
            }
        }
    }

    private void setLimiterState(QueryLimiterImpl.State state) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = limiter.getClass().getDeclaredMethod("setState", QueryLimiterImpl.State.class);
        method.setAccessible(true);
        method.invoke(limiter, state);
    }

}
