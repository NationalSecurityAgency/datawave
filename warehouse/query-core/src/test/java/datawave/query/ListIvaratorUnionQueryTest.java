package datawave.query;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.base.Preconditions;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.async.event.VisitorFunction;
import datawave.query.util.AbstractIngest;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;

/**
 * Asserts that a union of equality terms returns the same documents whether the union is executed as a union of field index iterators or is pushed down into a
 * list ivarator.
 * <p>
 * The pushdown is performed by the {@link VisitorFunction}, not by the {@link DefaultQueryPlanner}, so the plan asserted by {@link #expectPlan(String)} is the
 * union in both cases. The distinguishing assertion is made against the query that exits the visitor function -- see
 * {@link #assertPlanExitingVisitorFunction()}.
 * <p>
 * The two tests differ only in {@code maxOrExpansionThreshold}:
 * <ul>
 * <li>{@link #testUnionIsNotPushedDownWhenThresholdIsHigh()} keeps the threshold above the number of terms in the union, so the union survives intact</li>
 * <li>{@link #testUnionIsPushedDownIntoListIvarator()} drops the threshold below the number of terms, so the union becomes a list ivarator</li>
 * </ul>
 * Both tests expect the same documents. A difference between them is a defect in the pushdown, not in the query.
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class ListIvaratorUnionQueryTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(ListIvaratorUnionQueryTest.class);

    /** The label of the query property marker that denotes a list ivarator */
    private static final String LIST_MARKER = "_List_";

    @TempDir
    public static Path folder;

    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient client;
    private static AbstractIngest ingest;

    /** The values written to FIELD_A, one per document */
    private static final List<String> VALUES = List.of("alpha", "bravo", "charlie", "delta", "echo", "foxtrot");

    /** The uuid of the document that holds the value at the same offset in {@link #VALUES} */
    private static final List<String> UUIDS = List.of("uuid-1", "uuid-2", "uuid-3", "uuid-4", "uuid-5", "uuid-6");

    /** Set when a test expects the visitor function to push the union down into a list ivarator */
    private boolean expectListIvarator = false;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected void extraConfigurations() {
        // no-op
    }

    @Override
    protected void extraAssertions() {
        assertPlanExitingVisitorFunction();
    }

    /**
     * {@link AbstractIngest} only populates the standard shard index, so the alternate index tables are not exercised here.
     *
     * @return the shard index table name
     */
    @Override
    protected List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(ListIvaratorUnionQueryTest.class.getName());
        client = new InMemoryAccumuloClient("", instance);

        ingest = new AbstractIngest(client, auths);

        ingest.registerField("UUID", new LcNoDiacriticsType());
        ingest.registerColumns("UUID", List.of("i", "e"));

        ingest.registerField("FIELD_A", new LcNoDiacriticsType());
        ingest.registerColumns("FIELD_A", List.of("i", "e"));

        for (int i = 0; i < VALUES.size(); i++) {
            ingest.writeFV(i + 1, "UUID", UUIDS.get(i));
            ingest.writeFV(i + 1, "FIELD_A", VALUES.get(i));
        }
    }

    @BeforeEach
    public void beforeEach() {
        setClientForTest(client);

        // an ivarator cannot be built without an hdfs configuration and a cache directory, and the visitor
        // function will not attempt a pushdown unless both are present
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        Preconditions.checkNotNull(hadoopConfig);
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(new IvaratorCacheDirConfig(folder.toUri().toString())));

        // raise the term thresholds out of the way so that the size of the union is governed only by
        // the pushdown threshold under test
        logic.setInitialMaxTermThreshold(1_000);
        logic.setIntermediateMaxTermThreshold(1_000);
        logic.setIndexedMaxTermThreshold(1_000);
        logic.setFinalMaxTermThreshold(1_000);

        // keep the fst threshold out of the way so the pushdown always produces an inline list of values
        logic.setMaxOrExpansionFstThreshold(1_000);

        expectListIvarator = false;

        givenDate(ingest.getDate());
    }

    /**
     * A pushdown threshold larger than the union prevents the union from being pushed down. The query executes as a union of field index iterators.
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testUnionIsNotPushedDownWhenThresholdIsHigh() throws Exception {
        logic.setMaxOrExpansionThreshold(VALUES.size() + 1);

        givenQuery(union());
        expectPlan(union());
        expectResultCount(VALUES.size());
        expectUUIDs(Set.copyOf(UUIDS));

        expectListIvarator = false;
        planAndExecuteQuery();
    }

    /**
     * A pushdown threshold smaller than the union causes the visitor function to replace the union with a list ivarator. The same documents must be returned.
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testUnionIsPushedDownIntoListIvarator() throws Exception {
        logic.setMaxOrExpansionThreshold(2);

        givenQuery(union());
        // the planner does not perform the pushdown, so the planned query is still the union
        expectPlan(union());
        expectResultCount(VALUES.size());
        expectUUIDs(Set.copyOf(UUIDS));

        expectListIvarator = true;
        planAndExecuteQuery();
    }

    /**
     * Build the union of equality terms, one term per value in {@link #VALUES}.
     *
     * @return the union
     */
    private static String union() {
        //  @formatter:off
        return VALUES.stream()
                        .map(value -> "FIELD_A == '" + value + "'")
                        .collect(Collectors.joining(" || "));
        //  @formatter:on
    }

    /**
     * Assert whether the query that exits the {@link VisitorFunction} contains a list ivarator.
     * <p>
     * The large fielded list pushdown runs per-scan-range inside the visitor function rather than in the planner, so it is invisible to
     * {@link #expectPlan(String)}. Running the visitor function over the planned query reproduces exactly the query that is shipped to the tablet server.
     */
    private void assertPlanExitingVisitorFunction() {
        String plan = planExitingVisitorFunction();
        log.info("plan exiting the visitor function: {}", plan);

        if (expectListIvarator) {
            assertTrue(plan.contains(LIST_MARKER), "expected a list ivarator in the plan exiting the visitor function but got: " + plan);
        } else {
            assertFalse(plan.contains(LIST_MARKER), "expected no list ivarator in the plan exiting the visitor function but got: " + plan);
        }
    }

    /**
     * Run the {@link VisitorFunction} over the planned query and return the rewritten query.
     *
     * @return the query that exits the visitor function
     */
    private String planExitingVisitorFunction() {
        assertInstanceOf(DefaultQueryPlanner.class, logic.getQueryPlanner());
        DefaultQueryPlanner planner = (DefaultQueryPlanner) logic.getQueryPlanner();

        IteratorSetting setting = new IteratorSetting(50, "query", QueryIterator.class);
        setting.addOption(QueryOptions.QUERY, logic.getConfig().getQueryString());

        List<Range> ranges = new ArrayList<>();
        ranges.add(new Range(ingest.getRow()));

        try {
            VisitorFunction function = new VisitorFunction(logic.getConfig(), planner.getMetadataHelper());
            return function.apply(setting, ranges).getOptions().get(QueryOptions.QUERY);
        } catch (Exception e) {
            throw new RuntimeException("failed to run the visitor function", e);
        }
    }
}
