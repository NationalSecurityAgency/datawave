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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.base.Preconditions;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.IpAddressType;
import datawave.data.type.LcType;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.async.event.VisitorFunction;
import datawave.query.util.AbstractIngest;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.TestIndexTableNames;

/**
 * Pins a defect in the large fielded list pushdown for top level document queries.
 * <p>
 * The query is an anchor union intersected with a union of addresses, {@code (A || B) && (IP1 || IP2 || ...)}, run against a top level document logic where the
 * addresses live on child documents. Both tests query identical data and differ only in {@code maxOrExpansionThreshold}:
 * <ul>
 * <li>{@link #testUnionIsNotPushedDownWhenThresholdIsHigh()} keeps the threshold above the size of the address union, so the union survives as a union of field
 * index iterators and every matching document is returned</li>
 * <li>{@link #testUnionIsPushedDownIntoListIvarator()} drops the threshold below the size of the address union, so the {@link VisitorFunction} replaces it with
 * a list ivarator - and <b>no documents are returned at all</b></li>
 * </ul>
 * <b>The defect:</b> {@code TLDIndexBuildingVisitor} overrides {@code visit(ASTEQNode)} so that an equality term builds a {@code TLDIndexIteratorBuilder},
 * which rolls a hit on a child document's field index entry up to its top level document uid. It does not override {@code ivarateList}, so the pushed down
 * union builds a plain {@code IndexListIteratorBuilder} instead. That ivarator emits the child uid unchanged, which never intersects the top level uids
 * produced by the anchor union, and the intersection empties out.
 * <p>
 * The expectations below record the <b>current, incorrect</b> behavior so that a fix is forced to update them. {@link #testUnionIsPushedDownIntoListIvarator()}
 * should return the same documents as {@link #testUnionIsNotPushedDownWhenThresholdIsHigh()}.
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

    /** the label of the query property marker that denotes a list ivarator */
    private static final String LIST_MARKER = "_List_";

    @TempDir
    public static Path folder;

    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient client;
    private static AbstractIngest ingest;

    /** number of top level documents written */
    private static final int DOCS = 6;
    /** number of child documents, each holding one address, written per top level document */
    private static final int ADDRESSES_PER_DOC = 2;
    /** documents 0..MATCHING-1 carry an anchor value, the rest are excluded by the anchor union */
    private static final int MATCHING = 4;

    /** every address written, and therefore every address queried */
    private static final List<String> ADDRESSES = new ArrayList<>();
    /** the uuid of each top level document, in write order */
    private static final List<String> UUIDS = new ArrayList<>();

    /** set when a test expects the visitor function to push the address union down into a list ivarator */
    private boolean expectListIvarator = false;

    @Autowired
    @Qualifier("TLDEventQuery")
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
     * The addresses resolve to whole shards rather than to individual documents, so the 'no uid' index is the relevant index format here.
     *
     * @return the 'no uid' index table name
     */
    @Override
    protected List<String> getIndexTableNames() {
        return List.of(TestIndexTableNames.NO_UID_INDEX);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(ListIvaratorUnionQueryTest.class.getName());
        client = new InMemoryAccumuloClient("", instance);

        ingest = new AbstractIngest(client, auths);

        ingest.registerField("UUID", new LcType());
        ingest.registerColumns("UUID", List.of("i", "e"));

        ingest.registerField("ANCHOR", new LcType());
        ingest.registerColumns("ANCHOR", List.of("i", "e"));

        ingest.registerField("IP", new IpAddressType());
        ingest.registerColumns("IP", List.of("i", "e"));

        for (int i = 0; i < DOCS; i++) {
            String uuid = String.format("uuid-%02d", i);
            UUIDS.add(uuid);

            // the top level document carries the uuid and the anchor
            ingest.writeFV(i + 1, "UUID", uuid);
            ingest.writeFV(i + 1, "ANCHOR", (i >= MATCHING) ? "other" : (i % 2 == 0 ? "left" : "right"));

            // the addresses are repeated values, so they live on child documents whose uid is the top
            // level uid suffixed with '.N'. their field index entries are written against that child uid.
            String topLevelUid = ingest.uid(i + 1);
            for (int j = 0; j < ADDRESSES_PER_DOC; j++) {
                String address = "200." + i + ".0." + (j + 1);
                ADDRESSES.add(address);
                ingest.writeFVForUid(ingest.getRow(), ingest.getDatatype(), topLevelUid + "." + (j + 1), "IP", address);
            }
        }

        // derive the alternate index tables, including the 'no uid' index used by this test
        new IndexIngestUtil().write(client, auths);
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

        // keep the fst threshold out of the way so a pushdown always produces an inline list of values
        logic.setMaxOrExpansionFstThreshold(1_000);

        expectListIvarator = false;

        givenDate(ingest.getDate());
        // a top level document holds several addresses, so only one of them is returned
        givenParameter(QueryParameters.LIMIT_FIELDS, "IP=1");
    }

    /**
     * A pushdown threshold larger than the address union prevents the union from being pushed down. Every matching top level document is returned.
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testUnionIsNotPushedDownWhenThresholdIsHigh() throws Exception {
        logic.setMaxOrExpansionThreshold(ADDRESSES.size() + 1);

        givenQuery(query());
        expectPlan(plannedQuery());
        expectResultCount(MATCHING);
        expectUUIDs(Set.copyOf(UUIDS.subList(0, MATCHING)));

        expectListIvarator = false;
        planAndExecuteQuery();
    }

    /**
     * A pushdown threshold smaller than the address union causes the visitor function to replace it with a list ivarator, and the query then returns nothing.
     * <p>
     * The correct expectations are the ones asserted by {@link #testUnionIsNotPushedDownWhenThresholdIsHigh()}: {@code MATCHING} documents carrying the same
     * uuids and hit terms. See the class javadoc for the defect this pins.
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testUnionIsPushedDownIntoListIvarator() throws Exception {
        // large enough to leave the two term anchor union alone, small enough to push the address union down
        logic.setMaxOrExpansionThreshold(3);

        givenQuery(query());
        // the pushdown happens in the visitor function, so the planned query is still the union
        expectPlan(plannedQuery());
        expectResultCount(MATCHING);

        expectListIvarator = true;
        planAndExecuteQuery();
    }

    /**
     * Build the query, an anchor union intersected with a union of every address written.
     *
     * @return the query
     */
    private static String query() {
        //  @formatter:off
        String addressUnion = ADDRESSES.stream()
                        .map(address -> "IP == '" + address + "'")
                        .collect(Collectors.joining(" || "));
        //  @formatter:on
        return "(ANCHOR == 'left' || ANCHOR == 'right') && (" + addressUnion + ")";
    }

    /**
     * Build the expected query plan. The planner normalizes each address to its zero padded form.
     *
     * @return the expected plan
     */
    private static String plannedQuery() {
        IpAddressType type = new IpAddressType();
        //  @formatter:off
        String addressUnion = ADDRESSES.stream()
                        .map(address -> "IP == '" + type.normalize(address) + "'")
                        .collect(Collectors.joining(" || "));
        //  @formatter:on
        return "(ANCHOR == 'left' || ANCHOR == 'right') && (" + addressUnion + ")";
    }

    /**
     * Assert whether the query that exits the {@link VisitorFunction} contains a list ivarator.
     * <p>
     * The large fielded list pushdown runs per scan range inside the visitor function rather than in the planner, so it is invisible to
     * {@link #expectPlan(String)}. Running the visitor function over the planned query reproduces the query that is shipped to the tablet server.
     */
    private void assertPlanExitingVisitorFunction() {
        String plan = planExitingVisitorFunction();

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
