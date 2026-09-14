package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
    private ActiveQueryTracker activeQueryTracker;

    @Mock
    private UserLimitProvider userLimitProvider;

    @Mock
    private QueryLogicGroupLimitProvider groupLimitProvider;

    @Mock
    private SystemLimitProvider systemLimitProvider;

    @Mock
    private QueryHeartbeatCache heartbeatCache;

    @DisplayName("Method checkLimits() throws an exception when")
    @Nested
    class CheckLimitExpectedExceptions {

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

        @DisplayName("the query limiter has the state UNINITIALIZED")
        @Test
        void uninitializedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.UNINITIALIZED);
            assertThatThrownBy(() -> limiter.checkLimits("userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Checking limits not allowed while limiter is in state UNINITIALIZED");
        }

        @DisplayName("the query limiter has the state CLOSED")
        @Test
        void closedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.CLOSED);
            assertThatThrownBy(() -> limiter.checkLimits("userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Checking limits not allowed while limiter is in state CLOSED");
        }
    }

    @DisplayName("Method markActive() throws an exception when")
    @Nested
    class MarkActiveExpectedExceptions {

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

        @DisplayName("the query limiter has the state UNINITIALIZED")
        @Test
        void uninitializedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.UNINITIALIZED);
            assertThatThrownBy(() -> limiter.markActive("queryId", "userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Marking queries active not allowed while limiter is in state UNINITIALIZED");
        }

        @DisplayName("the query limiter has the state CLOSED")
        @Test
        void closedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.CLOSED);
            assertThatThrownBy(() -> limiter.markActive("queryId", "userA", "SYSTEM-01", "EventQueryLogic")).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Marking queries active not allowed while limiter is in state CLOSED");
        }
    }

    @DisplayName("Method markActive()")
    @Nested
    class MarkActiveBasicFunctionality {

        @DisplayName("Normalizes the user DN, system, and query logic")
        @Test
        void normalizesArgs() throws Exception {
            setLimiterState(QueryLimiterImpl.State.ACTIVE);

            limiter.markActive(" queryId ", " CN=UserA ", " Artemis-01 ", " TLDQueryLogic ");

            verify(activeQueryTracker).trackQuery(eq("queryId"), eq("cn=usera"), eq("Artemis-01"), eq("TLDQueryLogic"), anyBoolean());
        }

        @DisplayName("Uses EMPTY_SYSTEM_FROM if the system is null")
        @Test
        void usesDefaultSystemIfNullSystem() throws Exception {
            setLimiterState(QueryLimiterImpl.State.ACTIVE);

            limiter.markActive("queryId", " CN=UserA ", null, " TLDQueryLogic ");

            verify(activeQueryTracker).trackQuery(eq("queryId"), eq("cn=usera"), eq("EMPTY_SYSTEM_FROM"), eq("TLDQueryLogic"), anyBoolean());
        }

        @DisplayName("Uses EMPTY_SYSTEM_FROM if the system is blank")
        @Test
        void usesDefaultSystemIfBlankSystem() throws Exception {
            setLimiterState(QueryLimiterImpl.State.ACTIVE);

            limiter.markActive("queryId", " CN=UserA ", "  ", " TLDQueryLogic ");

            verify(activeQueryTracker).trackQuery(eq("queryId"), eq("cn=usera"), eq("EMPTY_SYSTEM_FROM"), eq("TLDQueryLogic"), anyBoolean());
        }
    }

    @DisplayName("Method getActiveQueries() throws an exception when")
    @Nested
    class GetActiveQueriesExpectedExceptions {

        @DisplayName("the query limiter has the state UNINITIALIZED")
        @Test
        void uninitializedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.UNINITIALIZED);
            assertThatThrownBy(() -> limiter.getActiveQueries()).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Fetching active queries not allowed while limiter is in state UNINITIALIZED");
        }

        @DisplayName("the query limiter has the state CLOSED")
        @Test
        void closedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.CLOSED);
            assertThatThrownBy(() -> limiter.getActiveQueries()).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Fetching active queries not allowed while limiter is in state CLOSED");
        }
    }

    @DisplayName("Method getActiveQueries()")
    @Nested
    class GetActiveQueriesBasicFunctionality {

        @DisplayName("Returns queries from the heartbeat cache in state ACTIVE")
        @Test
        void activeState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.ACTIVE);
            limiter.getActiveQueries();
            verify(heartbeatCache).getQueryIds();
        }

        @DisplayName("Returns queries from the heartbeat cache in state IDLE")
        @Test
        void idleState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.IDLE);
            limiter.getActiveQueries();
            verify(heartbeatCache).getQueryIds();
        }
    }

    @DisplayName("Method markInactive(Collection<String>) throws an exception when")
    @Nested
    class MarkInactiveCollectionExpectedExceptions {

        @DisplayName("The queryIds collection is null")
        @Test
        void nullQueryIds() {
            assertThatThrownBy(() -> limiter.markInactive((Collection<String>) null)).isInstanceOf(NullPointerException.class)
                            .hasMessage("queryIds cannot be null");
        }

        @DisplayName("The queryIds collection contains a null queryId")
        @Test
        void nullQueryId() {
            Set<String> set = new HashSet<>();
            set.add(null);
            assertThatThrownBy(() -> limiter.markInactive(set)).isInstanceOf(IllegalArgumentException.class).hasMessage("queryId cannot be null or blank");
        }

        @DisplayName("The queryIds collection contains a blank queryId")
        @Test
        void blankQueryId() {
            assertThatThrownBy(() -> limiter.markInactive(Set.of("  "))).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("queryId cannot be null or blank");
        }

        @DisplayName("the query limiter has the state UNINITIALIZED")
        @Test
        void uninitializedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.UNINITIALIZED);
            assertThatThrownBy(() -> limiter.markInactive(Set.of())).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Marking queries inactive not allowed while limiter is in state UNINITIALIZED");
        }

        @DisplayName("the query limiter has the state CLOSED")
        @Test
        void closedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.CLOSED);
            assertThatThrownBy(() -> limiter.markInactive(Set.of())).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Marking queries inactive not allowed while limiter is in state CLOSED");
        }
    }

    @DisplayName("Method markInactive(Collection<String>)")
    @Nested
    class MarkInactiveCollectionBasicFunctionality {

        @Test
        void normalizesQueryIds() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.ACTIVE);
            Set<String> queryIds = Set.of("  queryId1  ");
            limiter.markInactive(queryIds);
            verify(heartbeatCache).stopAndRemove(eq(Set.of("queryId1")));
        }

        @DisplayName("Stops and removes queries in the heartbeat cache in state ACTIVE")
        @Test
        void activeState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.ACTIVE);
            Set<String> queryIds = Set.of("queryId1");
            limiter.markInactive(queryIds);
            verify(heartbeatCache).stopAndRemove(eq(queryIds));
        }

        @DisplayName("Stops and removes queries in the heartbeat cache in state IDLE")
        @Test
        void idleState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.IDLE);
            Set<String> queryIds = Set.of("queryId1");
            limiter.markInactive(queryIds);
            verify(heartbeatCache).stopAndRemove(eq(queryIds));
        }
    }

    @DisplayName("Method markInactive(String) throws an exception when")
    @Nested
    class MarkInactiveSingleExpectedExceptions {

        @DisplayName("The queryId is null")
        @Test
        void nullQueryId() {
            assertThatThrownBy(() -> limiter.markInactive((String) null)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("queryId cannot be null or blank");
        }

        @DisplayName("The queryId is blank")
        @Test
        void blankQueryIds() {
            assertThatThrownBy(() -> limiter.markInactive("   ")).isInstanceOf(IllegalArgumentException.class).hasMessage("queryId cannot be null or blank");
        }

        @DisplayName("the query limiter has the state UNINITIALIZED")
        @Test
        void uninitializedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.UNINITIALIZED);
            assertThatThrownBy(() -> limiter.markInactive("queryId")).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Marking queries inactive not allowed while limiter is in state UNINITIALIZED");
        }

        @DisplayName("the query limiter has the state CLOSED")
        @Test
        void closedState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.CLOSED);
            assertThatThrownBy(() -> limiter.markInactive("queryId")).isInstanceOf(IllegalStateException.class)
                            .hasMessage("Marking queries inactive not allowed while limiter is in state CLOSED");
        }
    }

    @DisplayName("Method markInactive(String)")
    @Nested
    class MarkInactiveSingleBasicFunctionality {

        @DisplayName("Stops and removes the query in the heartbeat cache in state ACTIVE")
        @Test
        void activeState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.ACTIVE);
            limiter.markInactive("queryId");
            verify(heartbeatCache).stopAndRemove(eq("queryId"));
        }

        @DisplayName("Stops and removes the query in the heartbeat cache in state IDLE")
        @Test
        void idleState() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
            setLimiterState(QueryLimiterImpl.State.IDLE);
            limiter.markInactive("queryId");
            verify(heartbeatCache).stopAndRemove(eq("queryId"));
        }
    }

    private <T> T getField(Object source, Class<T> clazz, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field field = source.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return clazz.cast(field.get(source));
    }

    private void setLimiterState(QueryLimiterImpl.State state) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = limiter.getClass().getDeclaredMethod("setState", QueryLimiterImpl.State.class);
        method.setAccessible(true);
        method.invoke(limiter, state);
    }

}
