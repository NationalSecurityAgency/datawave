package datawave.webservice.query.limit;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Contains tests that effectively stress test the {@link QueryLimiter} when multiple threads are recording new active queries.
 */
class QueryLimiterConcurrencyTest {

    private static final Logger log = Logger.getLogger(QueryLimiterConcurrencyTest.class);

    private static final Random random = new Random();

    // The minimum random number that may be created.
    private static final int MIN_RANDOM = 1;

    // The maximum random number that may be created.
    private static final int MAX_RANDOM = 5;

    // The random number generated for interval will be multiplied by this number to get the initial time that a thread should sleep between creation attempts.
    private static final long CREATION_INTERVAL_MULTIPLIER = 100L;

    // Due to the concurrent nature of these tests, it is possible for a few extra queries to be allowed to be created that, in a perfect world, should not have
    // been. For instance, perhaps when checking the limits for a candidate query, Zookeeper was updated with information about a new query that is not captured
    // when checking the limits of the candidate query.
    //
    // This number represents maximum amount of time allowed to elapse between extra queries that should not have been created to the last valid query that
    // is expected to be a valid creation. A certain fudge factor is required for testing simultaneous query creations.
    private static final long SIMULTANEOUS_QUERY_CREATION_ALLOWANCE = 100L;

    // Set this to true to print the attempt results for debugging purposes.
    private static final boolean printAttempts = false;

    private static ExecutorService executor;

    // A flag that when set to true, will force any threads submitted to the executor to stop.
    private final AtomicBoolean forceStop = new AtomicBoolean(false);
    private final List<QueryCreationTask> tasks = new ArrayList<>();

    private QueryLimitConfiguration limitConfig;
    private static final Map<String,QueryLimiter> serversToLimiters = new HashMap<>();
    private List<QueryCreationAttempt> attempts;
    private TestingServer server;

    @BeforeAll
    static void beforeAll() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Thread-%d").build();
        executor = Executors.newFixedThreadPool(5, threadFactory);
    }

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
    }

    @AfterEach
    void tearDown() throws IOException {
        serversToLimiters.values().forEach(QueryLimiter::shutdown);
        serversToLimiters.clear();

        if (server != null) {
            server.close();
        }

        limitConfig = null;
        tasks.clear();
        attempts = null;
    }

    @AfterAll
    static void afterAll() {
        executor.shutdown();
    }

    /**
     * Return a randomized interval between 100-500 milliseconds.
     */
    private static long getRandomInterval() {
        return (random.nextInt((MAX_RANDOM - MIN_RANDOM)) + MIN_RANDOM) * CREATION_INTERVAL_MULTIPLIER;
    }

    /**
     * Verify that when the max default limit for a user is reached across all systems, that additional queries are not allowed to be created for the user.
     */
    @Test
    void testDefaultUserLimit() throws InterruptedException, ExecutionException {
        // Establish a default user limit of 20.
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(20);
        givenLimitConfig(config);

        // Create three separate tasks representing different servers. Each 'server' will receive 10 client requests. This means we will attempt to create a
        // query for the same user 30 times.
        List<String> users = List.of("cn=testUser, c=us");
        List<String> queryLogics = List.of("EventQuery");
        givenTask(createRequests(10, users, List.of("server-01"), queryLogics));
        givenTask(createRequests(10, users, List.of("server-02"), queryLogics));
        givenTask(createRequests(10, users, List.of("server-03"), queryLogics));

        // Execute the query creation attempts.
        executeTasks();

        printAttempts(this.attempts);

        // Assert successes/failures.
        assertSucceeded(20, this.attempts);
    }

    /**
     * Verify that when the max default limit for a system is reached, that additional queries are not allowed to be created on the system.
     */
    @Test
    void testDefaultSystemLimit() throws ExecutionException, InterruptedException {
        // Establish a default system limit of 10.
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(10);
        config.setDefaultUserQueryLimit(15);
        givenLimitConfig(config);

        // Create three separate tasks targeting the same server. Each task will attempt to create 5 client requests for random users and query logics.
        List<String> users = List.of("cn=testUserA, c=us", "cn=testUserB, c=us", "cn=testUserC, c=us");
        List<String> queryLogics = List.of("EventQuery", "TLDLogicQuery", "EdgeQuery");
        List<String> servers = List.of("server-01");
        givenTask(createRequests(5, users, servers, queryLogics));
        givenTask(createRequests(5, users, servers, queryLogics));
        givenTask(createRequests(5, users, servers, queryLogics));

        // Execute the query creation attempts.
        executeTasks();

        printAttempts(this.attempts);

        // Assert successes/failures.
        assertSucceeded(10, this.attempts);
    }

    /**
     * Verify that when the max default limit for a query logic group is reached for a user across all systems, that the user may not create additional queries
     * for that query logic group.
     */
    @Test
    void testQueryLogicGroupLimit() throws ExecutionException, InterruptedException {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(15);

        // Set a limit of 10 TLD queries per user across all systems.
        QueryLogicGroupLimitConfiguration groupLimitConfig = new QueryLogicGroupLimitConfiguration();
        groupLimitConfig.setGroupName("TLD");
        groupLimitConfig.setQueryLogicPattern("TLD.*");
        groupLimitConfig.setQueryLimit(10);
        config.setQueryLogicGroupConfigs(List.of(groupLimitConfig));

        givenLimitConfig(config);

        // Create three separate tasks targeting different users and servers, with different varieties of 'TLD' queries.
        List<String> users = List.of("cn=testUserA, c=us", "cn=testUserB, c=us", "cn=testUserC, c=us");
        List<String> queryLogics = List.of("TLDExtendedQuery", "TLDLogicQuery", "TLDVariant");
        List<String> servers = List.of("server-01", "server-02", "server-03");
        givenTask(createRequests(20, users, servers, queryLogics));
        givenTask(createRequests(20, users, servers, queryLogics));
        givenTask(createRequests(20, users, servers, queryLogics));

        // Execute the query creation attempts.
        executeTasks();

        // Split the attempts up by user.
        ArrayListMultimap<String,QueryCreationAttempt> attemptsPerUser = ArrayListMultimap.create();
        for (QueryCreationAttempt attempt : attempts) {
            attemptsPerUser.put(attempt.clientRequest.userDn, attempt);
        }

        // Assert that at most, 10 query creations succeeded per user.
        for (String user : attemptsPerUser.keySet()) {
            if (printAttempts) {
                if (log.isDebugEnabled()) {
                    log.debug("Attempts for user " + user);
                }
                printAttempts(attemptsPerUser.get(user));
            }

            assertSucceeded(10, attemptsPerUser.get(user));
        }
    }

    /**
     * Verify that when an individual user reaches their custom query limit across multiple systems, that they are not allowed to create any more queries.
     */
    @Test
    void testCustomUserLimit() throws ExecutionException, InterruptedException {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(20);

        // Set a limit of 10 queries for the user testUserA.
        UserLimitConfiguration userLimitConfig = new UserLimitConfiguration();
        userLimitConfig.setUserDn("cn=testUserA, c=us");
        userLimitConfig.setQueryLimit(10);
        config.setUserConfigs(List.of(userLimitConfig));
        givenLimitConfig(config);

        // Create three separate tasks targeting different users, query logics, and systems. Each task will attempt to create 4 queries for the target user,
        // and 3 queries for other users, in randomized order.
        List<String> targetUser = List.of("cn=testUserA, c=us");
        List<String> users = List.of("cn=testUserB, c=us", "cn=testUserC, c=us");
        List<String> queryLogics = List.of("EventQuery", "TLDLogicQuery", "EdgeQuery");
        List<String> servers = List.of("server-01");

        List<ClientRequest> requests = createRequests(4, targetUser, servers, queryLogics);
        requests.addAll(createRequests(3, users, servers, queryLogics));
        Collections.shuffle(requests);
        givenTask(requests);

        servers = List.of("server-02");
        requests = createRequests(4, targetUser, servers, queryLogics);
        requests.addAll(createRequests(3, users, servers, queryLogics));
        Collections.shuffle(requests);
        givenTask(requests);

        servers = List.of("server-03");
        requests = createRequests(4, targetUser, servers, queryLogics);
        requests.addAll(createRequests(3, users, servers, queryLogics));
        Collections.shuffle(requests);
        givenTask(requests);

        // Execute the query creation attempts.
        executeTasks();

        // Split the attempts into two lists, those made for the target user, and all other attempts.
        List<QueryCreationAttempt> targetUserAttempts = new ArrayList<>();
        List<QueryCreationAttempt> otherAttempts = new ArrayList<>();
        for (QueryCreationAttempt attempt : this.attempts) {
            if (attempt.clientRequest.userDn.equals("cn=testUserA, c=us")) {
                targetUserAttempts.add(attempt);
            } else {
                otherAttempts.add(attempt);
            }
        }

        // Assert successes/failures.
        if (printAttempts) {
            log.debug("Attempts for user: cn=testUserA, c=us");
            printAttempts(targetUserAttempts);
        }
        assertSucceeded(10, targetUserAttempts);

        if (printAttempts) {
            log.debug("All other attempts");
            printAttempts(otherAttempts);
        }

        assertAllSucceeded(otherAttempts);
    }

    /**
     * Verify that when a system reaches a custom query limit across multiple systems, that no more queries may be created on the system.
     */
    @Test
    void testCustomSystemLimit() throws ExecutionException, InterruptedException {
        QueryLimitConfiguration config = new QueryLimitConfiguration();
        config.setDefaultSystemQueryLimit(100);
        config.setDefaultUserQueryLimit(20);

        SystemLimitConfiguration systemLimitConfig = new SystemLimitConfiguration();
        systemLimitConfig.setSystemPattern("server-01");
        systemLimitConfig.setQueryLimit(5);
        config.setSystemConfigs(List.of(systemLimitConfig));
        givenLimitConfig(config);

        // Create three separate tasks targeting the same server. Each task will attempt to create 5 client requests for random users and query logics.
        List<String> users = List.of("cn=testUserA, c=us", "cn=testUserB, c=us", "cn=testUserC, c=us");
        List<String> queryLogics = List.of("EventQuery", "TLDLogicQuery", "EdgeQuery");
        List<String> servers = List.of("server-01");
        givenTask(createRequests(5, users, servers, queryLogics));
        givenTask(createRequests(5, users, servers, queryLogics));
        givenTask(createRequests(5, users, servers, queryLogics));

        // Execute the query creation attempts.
        executeTasks();

        printAttempts(this.attempts);

        // Assert successes/failures.
        assertSucceeded(5, this.attempts);
    }

    private void printAttempts(Collection<QueryCreationAttempt> attempts) {
        if (printAttempts) {
            for (QueryCreationAttempt attempt : attempts) {
                String line = attempt.timestamp + ", queryId:" + attempt.queryId + ", system:" + attempt.clientRequest.system + ", userDn:"
                                + attempt.clientRequest.userDn + ", queryLogic:" + attempt.clientRequest.queryLogic + ": "
                                + (attempt.succeeded ? "SUCCESS" : "FAILURE");
                log.debug(line);
            }
        }
    }

    private void givenLimitConfig(QueryLimitConfiguration config) {
        this.limitConfig = config;
    }

    private void givenTask(List<ClientRequest> requests) {
        // @formatter:off
        this.tasks.add(QueryCreationTask.builder()
                        .withForceStop(forceStop)
                        .withRequests(requests)
                        .build());
        // @formatter:on
    }

    /**
     * Execute the tasks, and collect and sort the results.
     */
    private void executeTasks() throws InterruptedException, ExecutionException {
        // Submit and invoke the tasks in parallel.
        List<Future<List<QueryCreationAttempt>>> futures = executor.invokeAll(tasks);

        // Obtain the creation attempts from each 'server'.
        List<QueryCreationAttempt> allAttempts = new ArrayList<>();
        for (Future<List<QueryCreationAttempt>> future : futures) {
            allAttempts.addAll(future.get());
        }

        // Sort the attempts by chronological order.
        Collections.sort(allAttempts);

        this.attempts = allAttempts;
    }

    /**
     * Assert that all attempts in the list succeeded.
     *
     * @param attempts
     *            the attempts to evaluate
     */
    private void assertAllSucceeded(List<QueryCreationAttempt> attempts) {
        assertSucceeded(attempts.size(), attempts);
    }

    /**
     * Assert that the first N attempts (where N = expectedSuccesses) in the list succeeded, while the remaining failed.
     *
     * @param expectedSuccesses
     *            the expected number of successes
     * @param attempts
     *            the attempts to evaluate
     */
    private void assertSucceeded(int expectedSuccesses, List<QueryCreationAttempt> attempts) {
        int i = 0;

        // Avoid any out-of-bound issues.
        if (expectedSuccesses >= attempts.size()) {
            expectedSuccesses = attempts.size();
        }

        long timestampOfLastValidSuccess = 0L;

        // Assert that the first N requests succeeded.
        for (; i < expectedSuccesses; i++) {
            assertTrue(attempts.get(i).succeeded, "Expected request " + i + " to have succeeded");
            timestampOfLastValidSuccess = attempts.get(i).timestamp;
        }

        // The remaining attempts should have failed, and not due to an exception.
        for (; i < attempts.size(); i++) {
            QueryCreationAttempt attempt = attempts.get(i);
            // If the attempt succeeded, this is likely due to a scenario where a new query was in the middle of being tracked in zookeeper while checking the
            // limits for another candidate query. In a perfect world, this wouldn't happen. Check if the excess queries were created within the time allowed
            // to be considered simultaneous.
            if (attempt.succeeded) {
                log.debug("Unexpected query creation success seen, likely due to simultaneous limit checks on different threads");
                long timeSinceLastValidCreation = attempt.timestamp - timestampOfLastValidSuccess;
                assertTrue(timeSinceLastValidCreation <= SIMULTANEOUS_QUERY_CREATION_ALLOWANCE,
                                "Simultaneous query creation allowance exceeded for query " + attempt.queryId + ": " + timeSinceLastValidCreation + "ms");
            } else {
                // Verify that if the attempt failed, it did not fail due to an exception
                assertNull(attempt.exception, "Expected request " + i + " to not have failed due to exception, but did: " + attempt.message);
            }

        }
    }

    private static class QueryCreationTask implements Callable<List<QueryCreationAttempt>> {

        private final AtomicBoolean forceStop;
        private final List<ClientRequest> requests;
        private final List<QueryCreationAttempt> attempts = new ArrayList<>();

        public static class Builder {
            private AtomicBoolean forceStop;
            private List<ClientRequest> requests;

            public Builder withForceStop(AtomicBoolean forceStop) {
                this.forceStop = forceStop;
                return this;
            }

            public Builder withRequests(List<ClientRequest> requests) {
                this.requests = requests;
                return this;
            }

            public QueryCreationTask build() {
                return new QueryCreationTask(this.forceStop, this.requests);
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        public QueryCreationTask(AtomicBoolean forceStop, List<ClientRequest> requests) {
            this.forceStop = forceStop;
            this.requests = requests;
        }

        @Override
        public List<QueryCreationAttempt> call() {
            log.debug("Starting");
            Iterator<ClientRequest> iterator = requests.iterator();
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                if (forceStop.get()) {
                    log.debug("forced stop");
                    break;
                }

                try {
                    QueryLimiter queryLimiter = serversToLimiters.get(request.system);

                    // Check if a limit has been met.
                    QueryLimiterResponse limiterResponse = queryLimiter.checkForLimits(request.userDn, request.system, request.queryLogic);

                    // If a limit has been met, record it.
                    if (limiterResponse.metLimit()) {
                        log.trace("Met limit: " + limiterResponse.getMessage());
                        attempts.add(QueryCreationAttempt.failed(request, limiterResponse.getMessage()));
                    } else {
                        // Otherwise 'create' a query and store the heartbeat to keep it alive until stopped.
                        String queryId = UUID.randomUUID().toString();
                        queryLimiter.countQueryTowardsLimits(queryId, request.userDn, request.system, request.queryLogic);
                        log.trace("Created query " + queryId);
                        attempts.add(QueryCreationAttempt.succeeded(request, queryId));
                    }

                    // If there are any more 'requests', sleep for a random interval.
                    if (iterator.hasNext()) {
                        long sleepInterval = getRandomInterval();
                        log.debug("Sleeping for " + sleepInterval + " seconds");
                        // noinspection BusyWait
                        Thread.sleep(sleepInterval);
                    }
                } catch (Exception e) {
                    attempts.add(QueryCreationAttempt.failed(request, e));
                    log.debug("failed", e);
                }

            }
            return attempts;
        }
    }

    private static class QueryCreationAttempt implements Comparable<QueryCreationAttempt> {

        private final boolean succeeded;
        private final ClientRequest clientRequest;
        private final String message;
        private final String queryId;
        private final long timestamp;
        private final Exception exception;

        public static QueryCreationAttempt failed(ClientRequest request, String message) {
            return new QueryCreationAttempt(false, request, message, null, null);
        }

        public static QueryCreationAttempt failed(ClientRequest request, Exception exception) {
            return new QueryCreationAttempt(false, request, exception.getMessage(), null, exception);
        }

        public static QueryCreationAttempt succeeded(ClientRequest request, String queryId) {
            return new QueryCreationAttempt(true, request, null, queryId, null);
        }

        private QueryCreationAttempt(boolean succeeded, ClientRequest clientRequest, String message, String queryId, Exception exception) {
            this.succeeded = succeeded;
            this.clientRequest = clientRequest;
            this.message = message;
            this.queryId = queryId;
            this.timestamp = System.currentTimeMillis();
            this.exception = exception;
        }

        @Override
        public int compareTo(QueryCreationAttempt o) {
            int comparable = Long.compare(timestamp, o.timestamp);

            if (comparable == 0) {
                comparable = Boolean.compare(o.succeeded, succeeded);
            }

            if (comparable == 0) {
                comparable = clientRequest.system.compareTo(o.clientRequest.system);
            }

            if (comparable == 0) {
                comparable = clientRequest.userDn.compareTo(o.clientRequest.userDn);
            }

            if (comparable == 0) {
                return clientRequest.queryLogic.compareTo(o.clientRequest.queryLogic);
            }

            return comparable;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", QueryCreationAttempt.class.getSimpleName() + "[", "]").add("succeeded=" + succeeded)
                            .add("clientRequest=" + clientRequest).add("queryId='" + queryId + "'").add("timestamp=" + timestamp).add("exception=" + exception)
                            .toString();
        }
    }

    private static class ClientRequest {
        private final String userDn;
        private final String queryLogic;
        private final String system;

        public ClientRequest(String userDn, String system, String queryLogic) {
            this.userDn = userDn;
            this.system = system;
            this.queryLogic = queryLogic;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", ClientRequest.class.getSimpleName() + "[", "]").add("userDn='" + userDn + "'").add("queryLogic='" + queryLogic + "'")
                            .add("serverId='" + system + "'").toString();
        }
    }

    private List<ClientRequest> createRequests(int numRequests, List<String> users, List<String> systems, List<String> queryLogics) {
        List<ClientRequest> requests = new ArrayList<>(numRequests);
        for (int i = 0; i < numRequests; i++) {
            int userIdx = random.nextInt(users.size());
            int systemIdx = random.nextInt(systems.size());
            int queryLogicIdx = random.nextInt(queryLogics.size());
            String system = systems.get(systemIdx);
            requests.add(new ClientRequest(users.get(userIdx), system, queryLogics.get(queryLogicIdx)));
            ensureLimiterExistsFor(system);
        }
        return requests;
    }

    private void ensureLimiterExistsFor(String system) {
        if (!serversToLimiters.containsKey(system)) {
            QueryLimiter limiter = new QueryLimiter();
            limiter.setZookeeperConfig(server.getConnectString());
            limiter.setConfiguration(this.limitConfig);
            limiter.setHeartbeatCache(new QueryHeartbeatCache());
            limiter.setup();
            serversToLimiters.put(system, limiter);
        }
    }
}
