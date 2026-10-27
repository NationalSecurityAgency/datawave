package datawave.query.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveAsyncOperationException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.util.time.DateHelper;

/**
 * Tests of {@link DatePartitionedQueryPlanner}'s failure paths, using a stubbed inner {@link DefaultQueryPlanner} so that behavior can be observed without a
 * real Accumulo/metadata backend. {@link StubQueryPlanner#process} stands in for the real (metadata-dependent) initial-planning step, and
 * {@link StubQueryPlanner#reprocess} stands in for each sub-plan's execution, both driven purely by test-supplied behavior.
 * <p>
 * {@link TestableDatePartitionedQueryPlanner} overrides {@link DatePartitionedQueryPlanner#getFieldsForQuery} to always return an empty field set, so that
 * {@link DatePartitionedQueryPlanner#getSubQueryDateRanges} always takes the "no index holes" fast path (a single sub-range covering the whole query date
 * range) without needing a real {@link MetadataHelper}.
 */
class DatePartitionedQueryPlannerFailureIT {

    private static final Date BEGIN = DateHelper.parse("20260701");
    private static final Date END = DateHelper.parse("20260701");

    private ShardQueryConfiguration newConfig() {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setBeginDate(BEGIN);
        config.setEndDate(END);
        Query settings = new QueryImpl();
        settings.setQuery("FOO == 'bar'");
        config.setQuery(settings);
        try {
            config.setQueryTree(JexlASTHelper.parseJexlQuery("FOO == 'bar'"));
        } catch (org.apache.commons.jexl3.parser.ParseException e) {
            throw new RuntimeException(e);
        }
        return config;
    }

    /** A {@link DatePartitionedQueryPlanner} that bypasses real field index hole lookups so no MetadataHelper is required. */
    private static class TestableDatePartitionedQueryPlanner extends DatePartitionedQueryPlanner {
        // run after the initial planning pass but before the sub-plans are registered, to simulate a close() landing in that window
        private Runnable onGetFieldsForQuery;

        TestableDatePartitionedQueryPlanner(DefaultQueryPlanner inner) {
            super(inner);
        }

        @Override
        protected Set<String> getFieldsForQuery(ASTJexlScript queryTree, MetadataHelper metadataHelper) {
            if (onGetFieldsForQuery != null) {
                onGetFieldsForQuery.run();
            }
            return Collections.emptySet();
        }
    }

    /** A {@link DefaultQueryPlanner} stub whose initial-planning and sub-plan-execution behavior is fully test-controlled. */
    private static class StubQueryPlanner extends DefaultQueryPlanner {
        private final List<StubQueryPlanner> clones = new ArrayList<>();
        private final List<StubQueryPlanner> closed = new ArrayList<>();
        private boolean throwOnReprocess;
        private boolean throwOnProcess;
        private boolean executorShutdown;
        private boolean reprocessed;
        private boolean processed;
        // the initial-planning flags this planner was handed, captured on entry to process()
        private Boolean sawGeneratePlanOnly;
        private Boolean sawExpandValues;
        private Boolean sawDeferPushdownPullup;
        // run while a sub-plan is being planned, to simulate a close() landing mid-flight
        private Runnable onReprocess;
        // run while the initial planning pass is in flight, to simulate a close() landing mid-flight
        private Runnable onProcess;
        // run once a clone has been created but before the caller has had a chance to register it, to simulate a close() landing in that window
        private Runnable onClone;

        @Override
        public StubQueryPlanner clone() {
            StubQueryPlanner copy = new StubQueryPlanner();
            copy.throwOnReprocess = this.throwOnReprocess;
            copy.throwOnProcess = this.throwOnProcess;
            copy.onReprocess = this.onReprocess;
            copy.onProcess = this.onProcess;
            clones.add(copy);
            if (onClone != null) {
                onClone.run();
            }
            return copy;
        }

        @Override
        public CloseableIterable<QueryData> process(GenericQueryConfiguration genericConfig, String query, Query settings, ScannerFactory scannerFactory) {
            this.processed = true;
            ShardQueryConfiguration config = (ShardQueryConfiguration) genericConfig;
            this.sawGeneratePlanOnly = config.isGeneratePlanOnly();
            this.sawExpandValues = config.isExpandValues();
            this.sawDeferPushdownPullup = config.isDeferPushdownPullup();
            if (onProcess != null) {
                onProcess.run();
            }
            if (throwOnProcess) {
                throw new IllegalStateException("stubbed initial planning failure");
            }
            this.plannedScript = query;
            return DefaultQueryPlanner.emptyCloseableIterator();
        }

        @Override
        public CloseableIterable<QueryData> reprocess(ShardQueryConfiguration config, Query settings, ScannerFactory scannerFactory)
                        throws DatawaveQueryException {
            this.reprocessed = true;
            if (onReprocess != null) {
                onReprocess.run();
            }
            if (throwOnReprocess) {
                throw new DatawaveQueryException("stubbed sub-plan failure");
            }
            return DefaultQueryPlanner.emptyCloseableIterator();
        }

        @Override
        public void close(GenericQueryConfiguration genericConfig, Query settings) {
            closed.add(this);
        }

        @Override
        public void shutdownExecutor() {
            executorShutdown = true;
        }
    }

    /**
     * When {@code SubPlanCallable.getUpdatedConfig()} throws before the sub-plan config is assigned, the failure must still be routed through the normal
     * per-sub-plan fault tolerance rather than escaping as a raw {@link NullPointerException} from the still-null config. A null {@link Query} on the config
     * triggers this, since {@code getUpdatedConfig} dereferences {@code originalConfig.getQuery().setId(...)}.
     */
    @Test
    void subPlanConfigCreationThrows() {
        StubQueryPlanner inner = new StubQueryPlanner();
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        config.setQuery(null);

        DatawaveQueryException e = assertThrows(DatawaveQueryException.class, () -> planner.process(config, "FOO == 'bar'", new QueryImpl(), null));
        assertInstanceOf(NullPointerException.class, e.getCause(), "expected the original failure to be preserved as the cause");
    }

    /**
     * A sub-plan that fails after its config was built must be reported the same way, with the underlying failure preserved.
     */
    @Test
    void subPlanExecutionThrows() {
        StubQueryPlanner inner = new StubQueryPlanner();
        inner.throwOnReprocess = true;
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();

        assertThrows(DatawaveQueryException.class, () -> planner.process(config, "FOO == 'bar'", config.getQuery(), null));
    }

    /**
     * close() must release every planner clone created during process() - the initial-planning clone and one per sub-range - each of which allocates its own
     * thread pool. The clones share the query with the inner planner, so only their thread pools are released; the inner planner gets the full close().
     */
    @Test
    void closeReleasesClonedPlanners() throws Exception {
        StubQueryPlanner inner = new StubQueryPlanner();
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        CloseableIterable<QueryData> iterable = planner.process(config, "FOO == 'bar'", config.getQuery(), null);
        // force the single sub-plan to be created
        iterable.iterator().hasNext();

        // one clone for the initial planning pass, one for the single sub-range
        assertEquals(2, inner.clones.size(), "expected an initial-planning clone and one sub-plan clone");

        planner.close(config, config.getQuery());

        assertTrue(inner.closed.contains(inner), "expected the original inner planner to have been closed");
        assertEquals(2, inner.clones.stream().filter(c -> c.executorShutdown).count(), "expected every cloned planner's executor to have been shut down");
        assertEquals(0, inner.clones.stream().filter(c -> c.closed.contains(c)).count(), "cloned planners share the query and must not be fully closed");
    }

    /**
     * A second call to process() must release the clones left behind by the first, rather than leaking them.
     */
    @Test
    void reprocessReleasesPreviousClones() throws Exception {
        StubQueryPlanner inner = new StubQueryPlanner();
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        planner.process(config, "FOO == 'bar'", config.getQuery(), null).iterator().hasNext();
        List<StubQueryPlanner> firstRoundClones = new ArrayList<>(inner.clones);

        planner.process(config, "FOO == 'bar'", config.getQuery(), null).iterator().hasNext();

        assertEquals(firstRoundClones.size(), firstRoundClones.stream().filter(c -> c.executorShutdown).count(),
                        "expected the previous round's clones to have been released");
    }

    private SubPlanCallable newSubPlanCallable(StubQueryPlanner inner, ShardQueryConfiguration config) {
        // A planned query always carries an id by the time sub-plans are built - getUpdatedConfig() copies it onto each sub-plan's config - so give the
        // hand-built config one here rather than tripping over it inside the callable.
        config.getQuery().setId(UUID.randomUUID());
        Map.Entry<Pair<Date,Date>,Set<String>> dateRange = Map.entry(Pair.of(BEGIN, END), Collections.emptySet());
        return new SubPlanCallable(inner, config, dateRange, null);
    }

    /**
     * A sub-plan that is released before it starts planning must refuse to plan at all. Planning would clone a planner and start its thread pool, and the
     * owning planner has already finished releasing clones by that point, so nothing would be left to shut that pool down.
     */
    @Test
    void releasedSubPlanRefusesToPlan() {
        StubQueryPlanner inner = new StubQueryPlanner();
        SubPlanCallable callable = newSubPlanCallable(inner, newConfig());

        callable.releasePlanner();

        DatawaveAsyncOperationException e = assertThrows(DatawaveAsyncOperationException.class, callable::call);
        assertInstanceOf(IllegalStateException.class, e.getCause(), "expected the release to be reported as the cause");
        assertTrue(inner.clones.stream().noneMatch(c -> c.reprocessed), "a released sub-plan must never start planning");
    }

    /**
     * A release that arrives while a sub-plan is planning must not shut the planner clone's thread pool down underneath it - the in-flight plan would then be
     * submitting work to a terminated pool. The shutdown is deferred until planning completes, and must actually happen at that point.
     */
    @Test
    void releaseDuringPlanningIsDeferredUntilPlanningCompletes() throws Exception {
        StubQueryPlanner inner = new StubQueryPlanner();
        SubPlanCallable callable = newSubPlanCallable(inner, newConfig());

        AtomicBoolean shutdownWhilePlanning = new AtomicBoolean();
        inner.onReprocess = () -> {
            callable.releasePlanner();
            shutdownWhilePlanning.set(inner.clones.get(0).executorShutdown);
        };

        callable.call();

        assertFalse(shutdownWhilePlanning.get(), "the thread pool must not be shut down while the sub-plan is still planning");
        assertTrue(inner.clones.get(0).executorShutdown, "the thread pool must be shut down once planning completes");
    }

    /**
     * Releasing the same sub-plan twice must shut its thread pool down only once.
     */
    @Test
    void releasingSubPlanTwiceShutsDownOnce() throws Exception {
        StubQueryPlanner inner = new StubQueryPlanner();
        SubPlanCallable callable = newSubPlanCallable(inner, newConfig());

        callable.call();
        callable.releasePlanner();
        inner.clones.get(0).executorShutdown = false;
        callable.releasePlanner();

        assertFalse(inner.clones.get(0).executorShutdown, "a second release must not shut the pool down again");
    }

    /**
     * A close() that arrives while the initial planning pass is in flight must not shut that clone's thread pool down underneath it - the pass is still
     * submitting work to that pool. The shutdown is deferred until the pass completes, and must actually happen at that point.
     */
    @Test
    void closeDuringInitialPlanningIsDeferredUntilPlanningCompletes() {
        StubQueryPlanner inner = new StubQueryPlanner();
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        AtomicBoolean shutdownWhilePlanning = new AtomicBoolean();
        inner.onProcess = () -> {
            planner.close(config, config.getQuery());
            shutdownWhilePlanning.set(inner.clones.get(0).executorShutdown);
        };

        // the outcome of process() is checked last, so that a pool shut down mid-plan is reported as such rather than masked by the abort assertion
        Exception thrown = null;
        try {
            planner.process(config, "FOO == 'bar'", config.getQuery(), null);
        } catch (Exception e) {
            thrown = e;
        }

        assertFalse(shutdownWhilePlanning.get(), "the thread pool must not be shut down while the initial pass is still planning");
        assertTrue(inner.clones.get(0).executorShutdown, "the thread pool must be shut down once the initial pass completes");
        // the close() leaves nothing to own the sub-plans, so planning aborts rather than building them
        assertInstanceOf(DatawaveQueryException.class, thrown, "planning must abort once close() has landed");
    }

    /**
     * A close() that arrives after a clone has been created but before process() has registered it must stop that clone from planning at all. Planning would
     * start a thread pool, and close() has already finished releasing clones by that point, so nothing would be left to shut that pool down.
     */
    @Test
    void closeBeforeInitialPlannerRegistrationAbortsPlanning() {
        StubQueryPlanner inner = new StubQueryPlanner();
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        AtomicBoolean closedOnce = new AtomicBoolean();
        inner.onClone = () -> {
            if (closedOnce.compareAndSet(false, true)) {
                planner.close(config, config.getQuery());
            }
        };

        assertThrows(DatawaveQueryException.class, () -> planner.process(config, "FOO == 'bar'", config.getQuery(), null));

        assertEquals(1, inner.clones.size(), "only the unregistered initial-planning clone should have been created");
        assertFalse(inner.clones.get(0).processed, "a clone created after close() must never start planning");
    }

    /**
     * A close() that arrives after the initial planning pass but before the sub-plans are registered must leave those sub-plans released, so that none of them
     * can go on to clone a planner and start its thread pool.
     */
    @Test
    void closeBeforeSubPlanRegistrationReleasesSubPlans() {
        StubQueryPlanner inner = new StubQueryPlanner();
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        planner.onGetFieldsForQuery = () -> planner.close(config, config.getQuery());

        assertThrows(DatawaveQueryException.class, () -> planner.process(config, "FOO == 'bar'", config.getQuery(), null));

        assertEquals(1, inner.clones.size(), "no sub-plan clone should have been created after close()");
        assertTrue(inner.clones.get(0).executorShutdown, "the initial-planning clone must still have been released");
    }

    /**
     * The initial planning pass runs with the expansion flags temporarily forced. They must be restored even when that pass fails, since the caller's config
     * outlives a failed plan.
     */
    @Test
    void initialPlanningFailureRestoresConfigFlags() {
        StubQueryPlanner inner = new StubQueryPlanner();
        inner.throwOnProcess = true;
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        config.setGeneratePlanOnly(false);
        config.setExpandValues(true);
        config.setDeferPushdownPullup(false);

        assertThrows(IllegalStateException.class, () -> planner.process(config, "FOO == 'bar'", config.getQuery(), null));

        StubQueryPlanner initialClone = inner.clones.get(0);
        assertTrue(initialClone.sawGeneratePlanOnly, "the initial planning pass must run with generatePlanOnly set");
        assertFalse(initialClone.sawExpandValues, "the initial planning pass must run with value expansion disabled");
        assertTrue(initialClone.sawDeferPushdownPullup, "the initial planning pass must run with pushdown/pullup deferred");

        assertFalse(config.isGeneratePlanOnly(), "generatePlanOnly must be restored after a failed initial plan");
        assertTrue(config.isExpandValues(), "expandValues must be restored after a failed initial plan");
        assertFalse(config.isDeferPushdownPullup(), "deferPushdownPullup must be restored after a failed initial plan");
    }

    @Test
    void applyRulesUnsupported() {
        DatePartitionedQueryPlanner planner = new DatePartitionedQueryPlanner();
        assertThrows(UnsupportedOperationException.class, () -> planner.applyRules(null, null, null, null));
    }

    @Test
    void processRejectsNonShardConfig() {
        DatePartitionedQueryPlanner planner = new DatePartitionedQueryPlanner(new StubQueryPlanner());
        GenericQueryConfiguration notAShardConfig = new GenericQueryConfiguration() {};
        assertThrows(ClassCastException.class, () -> planner.process(notAShardConfig, "FOO == 'bar'", new QueryImpl(), null));
    }

    @Test
    void cloneCopiesInnerPlannerAndPlans() throws Exception {
        StubQueryPlanner inner = new StubQueryPlanner();
        DatePartitionedQueryPlanner planner = new DatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        TestableDatePartitionedQueryPlanner testable = new TestableDatePartitionedQueryPlanner(inner);
        testable.process(config, "FOO == 'bar'", config.getQuery(), null);

        DatePartitionedQueryPlanner copy = testable.clone();

        assertNotSame(testable, copy);
        assertNotSame(testable.getQueryPlanner(), copy.getQueryPlanner());
        assertNotNull(copy.getQueryPlanner());
        assertEquals(testable.getInitialPlan(), copy.getInitialPlan());
        assertEquals(testable.getPlannedScript(), copy.getPlannedScript());
    }
}
