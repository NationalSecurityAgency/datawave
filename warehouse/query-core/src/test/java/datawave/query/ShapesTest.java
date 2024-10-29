package datawave.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SeekingFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.ShapesIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * A set of tests that emphasize the influence of datatypes on query planning and execution
 * <p>
 * Data is from {@link ShapesIngest} test set.
 * <p>
 * <b>Note:</b> This test class does NOT use of the {@link RebuildingScannerTestHelper}. That helper class makes use of the Apache Common's
 * {@link IteratorChain} in a way that is incompatible with Accumulo's {@link SeekingFilter}. Namely, during a rebuild on a next call the ScannerHelper's call
 * to 'ChainIterator.next' will swap in a whole new seeking filter in a way that causes the call to 'range.clip' on SeekingFilter#222 to return null.
 */
public abstract class ShapesTest {

    private static final Logger log = Logger.getLogger(ShapesTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Collections.singleton(auths);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    protected KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private AccumuloClient clientForTest;

    public void setClientForTest(AccumuloClient client) {
        this.clientForTest = client;
    }

    // used for declarative style tests
    private String query;
    private Map<String,String> parameters = new HashMap<>();
    private Set<String> expected = new HashSet<>();
    private Set<Document> results = new HashSet<>();

    // useful collections
    private final Set<String> triangleUids = Sets.newHashSet(ShapesIngest.acuteUid, ShapesIngest.equilateralUid, ShapesIngest.isoscelesUid);
    private final Set<String> quadrilateralUids = Sets.newHashSet(ShapesIngest.squareUid, ShapesIngest.rectangleUid, ShapesIngest.rhomboidUid,
                    ShapesIngest.rhombusUid, ShapesIngest.trapezoidUid, ShapesIngest.kiteUid);
    private final Set<String> otherUids = Sets.newHashSet(ShapesIngest.pentagonUid, ShapesIngest.hexagonUid, ShapesIngest.octagonUid);
    private final Set<String> allUids = createSet(triangleUids, quadrilateralUids, otherUids);

    private final Set<String> allTypes = Sets.newHashSet("triangle", "quadrilateral", "pentagon", "hexagon", "octagon");

    @RunWith(Arquillian.class)
    public static class ShardRange extends ShapesTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            InMemoryInstance i = new InMemoryInstance(ShardRange.class.getName());
            client = new InMemoryAccumuloClient("", i);

            ShapesIngest.writeData(client, ShapesIngest.RangeType.SHARD);

            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Before
        public void beforeEach() {
            setClientForTest(client);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends ShapesTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            InMemoryInstance i = new InMemoryInstance(DocumentRange.class.getName());
            client = new InMemoryAccumuloClient("", i);

            ShapesIngest.writeData(client, ShapesIngest.RangeType.DOCUMENT);

            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Before
        public void beforeEach() {
            setClientForTest(client);
        }
    }

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        //  @formatter:off
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class)
                        .deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
        //  @formatter:on
    }

    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        resetState();
    }

    @After
    public void after() {
        resetState();
    }

    private void resetState() {
        query = null;
        if (logic != null) {
            logic.setReduceIngestTypes(false);
            logic.setRebuildDatatypeFilter(false);
            logic.setPruneQueryByIngestTypes(false);
        }
        parameters.clear();
        expected.clear();
        results.clear();
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    protected void runTestQuery(String query, Map<String,String> extraParameters, Set<String> expected) throws Exception {
        withQuery(query);
        withParameters(extraParameters);
        withExpected(expected);
        planQuery();
        executeQuery();
        assertUuids();
    }

    public ShapesTest withQuery(String query) {
        this.query = query;
        return this;
    }

    public ShapesTest withParameter(String key, String value) {
        parameters.put(key, value);
        return this;
    }

    public ShapesTest withParameters(Map<String,String> parameters) {
        this.parameters = parameters;
        return this;
    }

    public ShapesTest withExpected(Set<String> expected) {
        this.expected = expected;
        return this;
    }

    public ShapesTest planAndExecuteQuery() throws Exception {
        planQuery();
        executeQuery();
        assertUuids();
        return this;
    }

    public void planQuery() throws Exception {
        try {
            QueryImpl settings = new QueryImpl();
            settings.setBeginDate(format.parse("20240201"));
            settings.setEndDate(format.parse("20240209"));
            settings.setPagesize(Integer.MAX_VALUE);
            settings.setQueryAuthorizations(auths.serialize());
            settings.setQuery(query);
            settings.setParameters(parameters);
            settings.setId(UUID.randomUUID());

            logic.setMaxEvaluationPipelines(1);

            GenericQueryConfiguration config = logic.initialize(clientForTest, settings, authSet);
            logic.setupQuery(config);
        } catch (Exception e) {
            log.info("exception while planning query: " + e);
            throw e;
        }
    }

    public ShapesTest executeQuery() {
        results = new HashSet<>();
        for (Map.Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            results.add(d);
        }
        return this;
    }

    public ShapesTest assertUuids() {
        assertNotNull(expected);
        assertNotNull(results);

        Set<String> found = new HashSet<>();
        for (Document result : results) {
            Attribute<?> attr = result.get("UUID");
            assertNotNull("result did not contain a UUID", attr);
            String uuid = getUUID(attr);
            found.add(uuid);
        }

        Set<String> missing = Sets.difference(expected, found);
        if (!missing.isEmpty()) {
            log.info("missing uuids: " + missing);
        }

        Set<String> extra = Sets.difference(found, expected);
        if (!extra.isEmpty()) {
            log.info("extra uuids: " + extra);
        }

        assertEquals(expected, found);
        return this;
    }

    public String getUUID(Attribute<?> attribute) {
        boolean typed = attribute instanceof TypeAttribute;
        assertTrue("Attribute was not a TypeAttribute, was: " + attribute.getClass(), typed);
        TypeAttribute<?> uuid = (TypeAttribute<?>) attribute;
        return uuid.getType().getDelegateAsString();
    }

    public void assertPlannedQuery(String query) {
        try {
            ASTJexlScript expected = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript plannedScript = logic.getConfig().getQueryTree();
            if (!TreeEqualityVisitor.isEqual(expected, plannedScript)) {
                log.info("expected: " + query);
                log.info("planned : " + logic.getConfig().getQueryString());
                fail("Planned query did not match expectation");
            }
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
    }

    public void assertDatatypeFilter(Set<String> expected) {
        assertNotNull(logic);
        assertNotNull(logic.getConfig());
        assertEquals(expected, logic.getConfig().getDatatypeFilter());
    }

    @SafeVarargs
    public final Set<String> createSet(Set<String>... sets) {
        Set<String> s = new HashSet<>();
        for (Set<String> set : sets) {
            s.addAll(set);
        }
        return s;
    }

    @Test
    public void testTriangles() throws Exception {
        withQuery("SHAPE == 'triangle'");
        withParameter(QueryParameters.HIT_LIST, "true");
        withExpected(triangleUids);
        planAndExecuteQuery();
    }

    @Test
    public void testQuadrilaterals() throws Exception {
        withQuery("SHAPE == 'quadrilateral'");
        withParameter(QueryParameters.HIT_LIST, "true");
        withExpected(quadrilateralUids);
        planAndExecuteQuery();
    }

    @Test
    public void testPentagon() throws Exception {
        withQuery("SHAPE == 'pentagon'");
        withParameter(QueryParameters.HIT_LIST, "true");
        withExpected(Sets.newHashSet(ShapesIngest.pentagonUid));
        planAndExecuteQuery();
    }

    @Test
    public void testHexagon() throws Exception {
        withQuery("SHAPE == 'hexagon'");
        withParameter(QueryParameters.HIT_LIST, "true");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
    }

    @Test
    public void testOctagon() throws Exception {
        withQuery("SHAPE == 'octagon'");
        withParameter(QueryParameters.HIT_LIST, "true");
        withExpected(Sets.newHashSet(ShapesIngest.octagonUid));
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilaterals() throws Exception {
        withQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        withParameter("QueryParameters.HIT_LIST", "true");
        Set<String> uids = new HashSet<>();
        uids.addAll(triangleUids);
        uids.addAll(quadrilateralUids);
        withExpected(uids);
        planAndExecuteQuery();
    }

    @Test
    public void testAllShapes() throws Exception {
        withQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        withParameter(QueryParameters.HIT_LIST, "true");
        withExpected(allUids);
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilateralsNoFilter() throws Exception {
        withQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        Set<String> uids = new HashSet<>();
        uids.addAll(triangleUids);
        uids.addAll(quadrilateralUids);
        withExpected(uids);
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilateralsCorrectFilter() throws Exception {
        withQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle,quadrilateral");
        Set<String> uids = new HashSet<>();
        uids.addAll(triangleUids);
        uids.addAll(quadrilateralUids);
        withExpected(uids);
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilateralsFilterForTriangles() throws Exception {
        withQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle");
        withExpected(triangleUids);
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilateralsFilterForQuadrilaterals() throws Exception {
        withQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "quadrilateral");
        withExpected(quadrilateralUids);
        planAndExecuteQuery();
    }

    @Test
    public void testTrailingRegexExpansionIntoSingleTerm() throws Exception {
        withQuery("TYPE =~ 'acu.*'");
        withExpected(Sets.newHashSet(ShapesIngest.acuteUid));
        planAndExecuteQuery();
        assertPlannedQuery("TYPE == 'acute'");
    }

    @Test
    public void testTrailingRegexExpansionIntoMultipleTerms() throws Exception {
        withQuery("TYPE =~ 'rhomb.*'");
        withExpected(Sets.newHashSet(ShapesIngest.rhombusUid, ShapesIngest.rhomboidUid));
        planAndExecuteQuery();
        assertPlannedQuery("TYPE == 'rhombus' || TYPE == 'rhomboid'");
    }

    @Test
    public void testTrailingRegexExpansionIntoMultipleDatatypes() {
        // TODO
    }

    @Test
    public void testTrailingRegexExpansionIntoMultipleDatatypesWithDatatypeFilter() {
        // TODO
    }

    @Test
    public void testLeadingRegexExpansionIntoSingleTerm() throws Exception {
        withQuery("SHAPE =~ '.*angle'");
        withExpected(triangleUids);
        planAndExecuteQuery();
        assertPlannedQuery("SHAPE == 'triangle'");
    }

    @Test
    public void testLeadingRegexExpansionIntoMultipleTerms() throws Exception {
        withQuery("SHAPE =~ '.*gon'");
        withExpected(otherUids);
        planAndExecuteQuery();
        assertPlannedQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
    }

    @Test
    public void testLeadingRegexExpansionIntoMultipleDatatypes() throws Exception {
        withQuery("SHAPE =~ '.*gon'");
        withExpected(otherUids);
        planAndExecuteQuery();
        assertPlannedQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
    }

    @Test
    public void testLeadingRegexExpansionIntoMultipleDatatypesWithDatatypeFilter() throws Exception {
        withQuery("SHAPE =~ '.*gon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "pentagon,octagon");
        withExpected(Sets.newHashSet(ShapesIngest.pentagonUid, ShapesIngest.octagonUid));
        planAndExecuteQuery();
        assertPlannedQuery("SHAPE == 'pentagon' || SHAPE == 'octagon'");
    }

    // simple query, no filter vs. filter from params, permutations of rebuild, reduce, and prune

    @Test
    public void testSimpleQueryNoFilterSpecified() throws Exception {
        withQuery("SHAPE == 'hexagon'");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Collections.emptySet());
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithRebuild() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        withQuery("SHAPE == 'hexagon'");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(allTypes);
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithReduce() throws Exception {
        logic.setReduceIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Collections.emptySet());
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithPrune() throws Exception {
        logic.setPruneQueryByIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(allTypes);
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithRebuildReduce() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setReduceIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(allTypes);
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithRebuildPrune() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setPruneQueryByIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(allTypes);
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithReducePrune() throws Exception {
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(allTypes);
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithRebuildReducePrune() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(allTypes);
    }

    // simple query with filter

    @Test
    public void testSimpleQueryFilterFromParameters() throws Exception {
        withQuery("SHAPE == 'hexagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "hexagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithRebuild() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        withQuery("SHAPE == 'hexagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "hexagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithReduce() throws Exception {
        logic.setReduceIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "hexagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithPrune() throws Exception {
        logic.setPruneQueryByIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "hexagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithRebuildReduce() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setReduceIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "hexagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithRebuildPrune() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setPruneQueryByIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "hexagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithReducePrune() throws Exception {
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "hexagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithRebuildReducePrune() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        withQuery("SHAPE == 'hexagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "hexagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    // intersection with reduction possible

    @Test
    public void testIntersectionNoFilter() throws Exception {
        withQuery("SHAPE == 'hexagon' && ONLY_HEX == 'hexa'");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertDatatypeFilter(Collections.emptySet());
    }

    @Test
    public void testFinalDatatypeFilterWhenNoneSpecified() throws Exception {
        withQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        withExpected(otherUids);
        planAndExecuteQuery();
        assertPlannedQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        assertDatatypeFilter(Collections.emptySet());
    }

    @Test
    public void testFinalDatatypeFilterFromParameters() throws Exception {
        withQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "pentagon,hexagon,octagon");
        withExpected(otherUids);
        planAndExecuteQuery();
        assertPlannedQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        assertDatatypeFilter(Sets.newHashSet("pentagon", "hexagon", "octagon"));
    }

    @Test
    public void testBuildDatatypeFilterFromQueryFields() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        withQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        withExpected(otherUids);
        planAndExecuteQuery();
        assertPlannedQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        // SHAPE is common across all five datatypes
        assertDatatypeFilter(Sets.newHashSet("triangle", "quadrilateral", "pentagon", "hexagon", "octagon"));
    }

    @Test
    public void testReduceIngestTypesWithEmptyDatatypeFilter() throws Exception {
        // this parameter will not replace an empty datatype filter
        logic.setReduceIngestTypes(true);
        withQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        withExpected(otherUids);
        planAndExecuteQuery();
        assertPlannedQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        assertDatatypeFilter(Collections.emptySet());
    }

    @Test
    public void testReduceIngestTypesWithDatatypeFilterFromParametersNoChange() throws Exception {
        // SHAPE is common to five datatypes, only three specified in parameter. Reducing does not change the filter.
        logic.setReduceIngestTypes(true);
        withQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "pentagon,hexagon,octagon");
        withExpected(otherUids);
        planAndExecuteQuery();
        assertPlannedQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        assertDatatypeFilter(Sets.newHashSet("pentagon", "hexagon", "octagon"));
    }

    @Test
    public void testReduceIngestTypesWithDatatypeFilterFromParameters() throws Exception {
        logic.setReduceIngestTypes(true);
        withQuery("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "pentagon,hexagon,octagon");
        withExpected(Sets.newHashSet(ShapesIngest.pentagonUid, ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        // octagon datatype is pruned but the query remains intact
        assertPlannedQuery("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        assertDatatypeFilter(Sets.newHashSet("pentagon", "hexagon"));
    }

    @Test
    public void testPruneIngestTypes() throws Exception {
        // octagon should be pruned given the fields unique to each datatype
        logic.setPruneQueryByIngestTypes(true);
        withQuery("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "pentagon,hexagon,octagon");
        withExpected(Sets.newHashSet(ShapesIngest.pentagonUid, ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        // octagon datatype is NOT pruned despite pruning the term from the query
        assertPlannedQuery("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        assertDatatypeFilter(Sets.newHashSet("pentagon", "hexagon"));
    }

    @Test
    public void testReduceAndPruneIngestTypes() throws Exception {
        // octagon datatype should be pruned given the fields unique to each datatype
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        withQuery("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "pentagon,hexagon,octagon");
        withExpected(Sets.newHashSet(ShapesIngest.pentagonUid, ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        // octagon datatype is pruned
        assertPlannedQuery("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        assertDatatypeFilter(Sets.newHashSet("pentagon", "hexagon"));
    }

    // test cases for when a user specifies a filter that does not match the query fields, a filter with more types

    @Test(expected = InvalidQueryException.class)
    public void testExclusiveFilter() throws Exception {
        withQuery("ONLY_HEX == 'hexa'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle");
        withExpected(Collections.emptySet());
        planAndExecuteQuery(); // datatype filter will not find ONLY_HEX and throw exception
    }

    @Test(expected = InvalidQueryException.class)
    public void testExclusiveFilterWithReduce() throws Exception {
        logic.setReduceIngestTypes(true);
        withQuery("ONLY_HEX == 'hexa'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle");
        withExpected(Collections.emptySet());
        planAndExecuteQuery(); // datatype filter will not find ONLY_HEX and throw exception
    }

    @Test(expected = InvalidQueryException.class)
    public void testExclusiveFilterWithRebuild() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        withQuery("ONLY_HEX == 'hexa'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle");
        withExpected(Collections.emptySet());
        planAndExecuteQuery(); // datatype filter will not find ONLY_HEX and throw exception
    }

    @Test(expected = InvalidQueryException.class)
    public void testExclusiveFilterWithPrune() throws Exception {
        logic.setPruneQueryByIngestTypes(true);
        withQuery("ONLY_HEX == 'hexa'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle");
        withExpected(Collections.emptySet());
        planAndExecuteQuery(); // datatype filter will not find ONLY_HEX and throw exception
    }

    @Test
    public void testFilterWithExtraTypes() throws Exception {
        withQuery("ONLY_HEX == 'hexa'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle,quadrilateral,pentagon,hexagon,octagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertPlannedQuery("ONLY_HEX == 'hexa'");
        assertDatatypeFilter(allTypes);
    }

    @Test
    public void testFilterWithExtraTypesWithReduce() throws Exception {
        logic.setReduceIngestTypes(true);
        withQuery("ONLY_HEX == 'hexa'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle,quadrilateral,pentagon,hexagon,octagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertPlannedQuery("ONLY_HEX == 'hexa'");
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testFilterWithExtraTypesWithRebuild() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        withQuery("ONLY_HEX == 'hexa'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle,quadrilateral,pentagon,hexagon,octagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertPlannedQuery("ONLY_HEX == 'hexa'");
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testFilterWithExtraTypesWithPrune() throws Exception {
        logic.setPruneQueryByIngestTypes(true);
        withQuery("ONLY_HEX == 'hexa'");
        withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle,quadrilateral,pentagon,hexagon,octagon");
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertPlannedQuery("ONLY_HEX == 'hexa'");
        assertDatatypeFilter(Sets.newHashSet("hexagon"));
    }

    @Test
    public void testPruneNestedTermAllPermutations() throws Exception {
        // natural prune will drop the ONLY_QUAD term
        logic.setPruneQueryByIngestTypes(true);
        String query = "ONLY_HEX == 'hexa' && (SHAPE == 'hexagon' || ONLY_QUAD == 'square')";
        withQuery(query);
        withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
        planAndExecuteQuery();
        assertPlannedQuery("ONLY_HEX == 'hexa' && SHAPE == 'hexagon'");
    }

    /**
     * A slightly larger test
     *
     * @throws Exception
     *             if something goes wrong
     */
    @Test
    public void testPermutations() throws Exception {
        String query = "ONLY_HEX == 'hexa' && (SHAPE == 'hexagon' || ONLY_QUAD == 'square')";
        String expectedPlan = "ONLY_HEX == 'hexa' && SHAPE == 'hexagon'";

        boolean[] pruneOptions = {false, true};
        boolean[] reduceOptions = {false, true};
        boolean[] rebuildOptions = {false, true};

        for (boolean pruneOption : pruneOptions) {
            for (boolean reduceOption : reduceOptions) {
                for (boolean rebuildOption : rebuildOptions) {
                    resetState();

                    logic.setPruneQueryByIngestTypes(pruneOption);
                    logic.setReduceIngestTypes(reduceOption);
                    logic.setRebuildDatatypeFilter(rebuildOption);
                    logic.getConfig().setDatatypeFilter(Collections.emptySet());

                    withQuery(query);
                    withExpected(Sets.newHashSet(ShapesIngest.hexagonUid));
                    planAndExecuteQuery();

                    if (pruneOption) {
                        assertPlannedQuery(expectedPlan);
                    } else {
                        assertPlannedQuery(query);
                    }
                }
            }
        }
    }

    @Test
    public void testSortQueryPreIndexWithImpliedCounts() throws Exception {
        try {
            // sorting via implied counts should push TYPE to the right of SHAPE
            withQuery("TYPE == 'pentagon' || SHAPE == 'triangle'");
            withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle,pentagon");

            Set<String> expectedUids = new HashSet<>(triangleUids);
            withExpected(expectedUids);

            disableAllSortOptions();
            logic.setSortQueryPreIndexWithImpliedCounts(true);
            planAndExecuteQuery();
            assertPlannedQuery("SHAPE == 'triangle' || TYPE == 'pentagon'");
        } finally {
            disableAllSortOptions();
        }
    }

    @Test
    public void testSortQueryPreIndexWithFieldCounts() throws Exception {
        try {
            // SHAPE cardinality for triangle and pentagon types is 23
            // TYPE cardinality for triangle and pentagon types is 21
            withQuery("SHAPE == 'triangle' || TYPE == 'pentagon'");
            withParameter(QueryParameters.DATATYPE_FILTER_SET, "triangle,pentagon");

            Set<String> expectedUids = new HashSet<>(triangleUids);
            withExpected(expectedUids);

            disableAllSortOptions();
            logic.setSortQueryPreIndexWithFieldCounts(true);
            planAndExecuteQuery();
            assertPlannedQuery("TYPE == 'pentagon' || SHAPE == 'triangle'");
        } finally {
            disableAllSortOptions();
        }
    }

    private void disableAllSortOptions() {
        logic.setSortQueryPreIndexWithImpliedCounts(false);
        logic.setSortQueryPreIndexWithFieldCounts(false);
        logic.setSortQueryPostIndexWithFieldCounts(false);
        logic.setSortQueryPostIndexWithTermCounts(false);
    }

}
