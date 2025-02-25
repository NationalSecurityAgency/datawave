package datawave.core.query.logic.composite;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.security.authorization.AuthorizationException;

public class CompositeQueryLogicResultsIteratorTest {
    @Test
    public void countDownLatchShortCircuitTest() throws Exception {
        final long pollTime = 1000;
        final long delay = 0;

        CompositeQueryLogicResultsIterator iterator = setup(pollTime, delay);
        long start = System.currentTimeMillis();
        iterator.hasNext();

        // the count-down latches finish so fast that there is never a poll time
        assertTrue(System.currentTimeMillis() - start < pollTime);
    }

    @Test
    public void minimumWaitTest() throws Exception {
        final long pollTime = 1000;
        final long delay = 1500;

        CompositeQueryLogicResultsIterator iterator = setup(pollTime, delay);
        long start = System.currentTimeMillis();
        iterator.hasNext();

        // find the first poll time more than the delay
        long minWaitTime = pollTime;
        while (minWaitTime < delay) {
            minWaitTime += pollTime;
        }

        // the wait time will always be at least the closest interval of the poll time rounded up
        assertTrue((System.currentTimeMillis() - start) >= minWaitTime);
    }

    private CompositeQueryLogicResultsIterator setup(long pollTime, long delay) throws Exception {
        CompositeQueryLogic logic = new StrippedCompositeQueryLogic();
        Map<String,QueryLogic<?>> logicMap = new HashMap<>();
        logicMap.put("alfred", new StubbedQueryLogic(delay));
        logicMap.put("ben", new StubbedQueryLogic(delay));
        logic.setQueryLogics(logicMap);

        Query settings = new QueryImpl();
        settings.setId(UUID.randomUUID());
        settings.setQuery("FIELD == 'value'");
        settings.setPagesize(1);

        GenericQueryConfiguration config = logic.initialize(null, settings, Collections.emptySet());
        logic.setupQuery(config);

        ArrayBlockingQueue queue = new ArrayBlockingQueue(1);
        CompositeQueryLogicResultsIterator iterator = new CompositeQueryLogicResultsIterator(logic, queue, pollTime, TimeUnit.MILLISECONDS);

        // small sleep to guarantee any init in the threads is finished for consistent test results
        Thread.sleep(25);

        return iterator;
    }

    /**
     * Stub this out for use in the above tests, not a functional QueryLogic
     */
    private static class StubbedQueryLogic extends BaseQueryLogic<Map.Entry<Key,Value>> {
        private final long delay;

        private StubbedQueryLogic(long delay) {
            this.delay = delay;
        }

        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set runtimeQueryAuthorizations) throws Exception {
            return getConfig();
        }

        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {

        }

        /**
         * Apply the delay to the hasNext call, and always return false
         *
         * @return
         */
        @Override
        public Iterator<Map.Entry<Key,Value>> iterator() {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return false;
                }

                @Override
                public Map.Entry<Key,Value> next() {
                    return null;
                }
            };
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return null;
        }

        @Override
        public AccumuloConnectionFactory.Priority getConnectionPriority() {
            return null;
        }

        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return null;
        }

        @Override
        public Set<String> getOptionalQueryParameters() {
            return Set.of();
        }

        @Override
        public Set<String> getRequiredQueryParameters() {
            return Set.of();
        }

        @Override
        public Set<String> getExampleQueries() {
            return Set.of();
        }
    }

    /**
     * Override to avoid exercising any authorizations code that is irrelevant for these tests
     */
    private static class StrippedCompositeQueryLogic extends CompositeQueryLogic {
        @Override
        public Set<Authorizations> updateRuntimeAuthorizationsAndQueryAuths(QueryLogic<?> logic, Query settings) throws AuthorizationException {
            // no-op
            return Collections.emptySet();
        }
    }
}
