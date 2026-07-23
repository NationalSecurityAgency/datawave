package datawave.query.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    }

    @Test
    void subPlanConfigCreationThrows() {
        // BUG: when SubPlanCallable.getUpdatedConfig() throws before subPlanConfig is assigned, both its own catch block and
        // DatePartitionedQueryIterable.populateNextIterable()'s finally block dereference the still-null subPlanConfig, so a bare NullPointerException
        // escapes process() instead of a DatawaveQueryException - bypassing the per-sub-plan fault tolerance entirely. This is reproduced here by giving
        // the config a null Query, which SubPlanCallable#getUpdatedConfig unconditionally dereferences via originalConfig.getQuery().setId(...).
        StubQueryPlanner inner = new StubQueryPlanner();
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        config.setQuery(null);

        assertThrows(NullPointerException.class, () -> planner.process(config, "FOO == 'bar'", new QueryImpl(), null));
    }

    @Test
    void closeDoesNotCloseClonedPlanners() throws Exception {
        // BUG: close() only closes the original inner planner, never the initialPlanner clone or the per-sub-range clones created during process() -
        // each of which spins up its own thread pool internally. This leaks ~10 threads per sub-range per query.
        StubQueryPlanner inner = new StubQueryPlanner();
        TestableDatePartitionedQueryPlanner planner = new TestableDatePartitionedQueryPlanner(inner);

        ShardQueryConfiguration config = newConfig();
        CloseableIterable<QueryData> iterable = planner.process(config, "FOO == 'bar'", config.getQuery(), null);
        // force the single sub-plan to be created
        iterable.iterator().hasNext();

        assertTrue(inner.clones.size() >= 1, "expected at least one clone to have been created");

        planner.close(config, config.getQuery());

        // close() does correctly close the original inner planner...
        assertTrue(inner.closed.contains(inner), "expected the original inner planner to have been closed");
        // ...but leaks every clone created during process() - the actual bug.
        assertEquals(0, inner.clones.stream().filter(c -> c.closed.contains(c)).count(), "no cloned planner should have been closed, but at least one was");
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
