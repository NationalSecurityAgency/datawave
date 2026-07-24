package datawave.query.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.Test;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
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
        TestableDatePartitionedQueryPlanner(DefaultQueryPlanner inner) {
            super(inner);
        }

        @Override
        protected Set<String> getFieldsForQuery(ASTJexlScript queryTree, MetadataHelper metadataHelper) {
            return Collections.emptySet();
        }
    }

    /** A {@link DefaultQueryPlanner} stub whose initial-planning and sub-plan-execution behavior is fully test-controlled. */
    private static class StubQueryPlanner extends DefaultQueryPlanner {
        private final List<StubQueryPlanner> clones = new ArrayList<>();
        private final List<StubQueryPlanner> closed = new ArrayList<>();
        private boolean throwOnReprocess;
        private boolean executorShutdown;

        @Override
        public StubQueryPlanner clone() {
            StubQueryPlanner copy = new StubQueryPlanner();
            copy.throwOnReprocess = this.throwOnReprocess;
            clones.add(copy);
            return copy;
        }

        @Override
        public CloseableIterable<QueryData> process(GenericQueryConfiguration genericConfig, String query, Query settings, ScannerFactory scannerFactory) {
            this.plannedScript = query;
            return DefaultQueryPlanner.emptyCloseableIterator();
        }

        @Override
        public CloseableIterable<QueryData> reprocess(ShardQueryConfiguration config, Query settings, ScannerFactory scannerFactory)
                        throws DatawaveQueryException {
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
