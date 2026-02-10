package datawave.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.user.SeekingFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.collections.iterators.IteratorChain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.ShapesIngest;
import datawave.query.util.TestIndexTableNames;
import datawave.test.MacTestUtil;
import datawave.util.TableName;

/**
 * A set of tests that emphasize the influence of datatypes on query planning and execution
 * <p>
 * Data is from {@link ShapesIngest} test set.
 * <p>
 * <b>Historical Note:</b> This test class does NOT use of the {@link RebuildingScannerTestHelper}. That helper class makes use of the Apache Common's
 * {@link IteratorChain} in a way that is incompatible with Accumulo's {@link SeekingFilter}. Namely, during a rebuild on a next call the ScannerHelper's call
 * to 'ChainIterator.next' will swap in a whole new seeking filter in a way that causes the call to 'range.clip' on SeekingFilter#222 to return null.
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
public class ShapesTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(ShapesTest.class);

    @TempDir
    public static Path folder;

    // temporary stores for when forcing ivarators via absurdly low index expansion thresholds
    private int maxUnfieldedExpansionThreshold;
    private int maxValueExpansionThreshold;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    protected static final String PASSWORD = "password";
    protected static MiniAccumuloCluster mac;

    // useful collections
    private final Set<String> triangleUids = Sets.newHashSet(ShapesIngest.acuteUid, ShapesIngest.equilateralUid, ShapesIngest.isoscelesUid);
    private final Set<String> quadrilateralUids = Sets.newHashSet(ShapesIngest.squareUid, ShapesIngest.rectangleUid, ShapesIngest.rhomboidUid,
                    ShapesIngest.rhombusUid, ShapesIngest.trapezoidUid, ShapesIngest.kiteUid);
    private final Set<String> otherUids = Sets.newHashSet(ShapesIngest.pentagonUid, ShapesIngest.hexagonUid, ShapesIngest.octagonUid);
    private final Set<String> allUids = createSet(triangleUids, quadrilateralUids, otherUids);

    private final Set<String> allTypes = Sets.newHashSet("triangle", "quadrilateral", "pentagon", "hexagon", "octagon");

    protected static AccumuloClient client = null;
    protected static TableOperations tops = null;

    private static final IndexIngestUtil ingestUtil = new IndexIngestUtil();

    private Set<String> expectedDatatypeFilter = null;
    private final Map<String,Class<?>> expectedNormalizers = new HashMap<>();
    private final Set<String> excludedFields = new HashSet<>();

    @BeforeAll
    public static void beforeAll() throws Exception {
        MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.toFile(), PASSWORD);
        cfg.setNumTservers(1);
        mac = new MiniAccumuloCluster(cfg);
        mac.start();

        client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));
        ShapesIngest.writeData(client, ShapesIngest.RangeType.SHARD);

        Authorizations auths = new Authorizations("ALL");
        ingestUtil.write(client, auths);

        tops = client.tableOperations();

        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        // resetState();

        setClientForTest(client);

        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        Preconditions.checkNotNull(hadoopConfig);
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(folder.toUri().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));

        logic.setMaxFieldIndexRangeSplit(1); // keep things simple

        // disable by default to make clear what tests actually require these settings
        logic.setSortQueryPostIndexWithTermCounts(false);
        logic.setCardinalityThreshold(0);

        // every test also exercises hit terms
        givenParameter(QueryParameters.HIT_LIST, "true");
        logic.setHitList(true);

        logic.setCollapseUids(false); // index table will either have uids or not

        givenDate("20240201", "20240209");
    }

    @AfterEach
    public void afterEach() {
        super.afterEach();
        resetState();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        mac.stop();
    }

    private void resetState() {
        if (logic != null) {
            logic.setReduceIngestTypes(false);
            logic.setRebuildDatatypeFilter(false);
            logic.setPruneQueryByIngestTypes(false);
        }
        expectedDatatypeFilter = null;
        expectedNormalizers.clear();
        excludedFields.clear();
    }

    @AfterAll
    public static void teardown() {
        TypeRegistry.reset();
    }

    /**
     * A wrapper around {@link #givenParameter(String, String)} for {@link QueryParameters#DATATYPE_FILTER_SET}.
     *
     * @param filter
     *            the datatype filter
     */
    public void givenDatatypeFilter(String filter) {
        givenParameter(QueryParameters.DATATYPE_FILTER_SET, filter);
    }

    /**
     * Wrapper around {@link #givenParameter(String, String)} when using {@link QueryParameters#RETURN_FIELDS}
     *
     * @param includes
     *            the set of fields to include
     */
    public void givenIncludeFields(Set<String> includes) {
        givenParameter(QueryParameters.RETURN_FIELDS, Joiner.on(',').join(includes));
    }

    /**
     * Wrapper around {@link #givenParameter(String, String)} when using {@link QueryParameters#DISALLOWLISTED_FIELDS}
     *
     * @param excludes
     *            the set of fields to exclude
     */
    public void givenExcludeFields(Set<String> excludes) {
        givenParameter(QueryParameters.DISALLOWLISTED_FIELDS, Joiner.on(',').join(excludes));
    }

    /**
     * Set the expected datatype filter.
     *
     * @param filter
     *            the expected datatype filter
     */
    public void expectDatatypeFilter(Set<String> filter) {
        if (expectedDatatypeFilter == null) {
            expectedDatatypeFilter = new HashSet<>();
        }
        expectedDatatypeFilter.addAll(filter);
    }

    /**
     * Set the expected field-normalizer pair
     *
     * @param field
     *            the field
     * @param clazz
     *            the normalizer
     */
    public void expectAttributeNormalizer(String field, Class<?> clazz) {
        expectedNormalizers.put(field, clazz);
    }

    public void expectNoFields(String... fields) {
        excludedFields.addAll(List.of(fields));
    }

    /**
     * For this test we must reset the datatype filter each time
     */
    @Override
    protected void extraConfigurations() {
        // the backing config is stateful, have to reset the datatype filter
        getLogic().getConfig().setDatatypeFilter(Collections.emptySet());
    }

    /**
     * Assert the final datatype filter, if configured
     */
    @Override
    protected void extraAssertions() {
        assertDatatypeFilter();
        assertAttributeNormalizer();
        assertFieldNotFound();
    }

    /**
     * Assert the final datatype filter against the expectation, if an expectation was set
     */
    public void assertDatatypeFilter() {
        if (expectedDatatypeFilter == null) {
            return;
        }

        assertNotNull(logic);
        assertNotNull(logic.getConfig());
        assertEquals(expectedDatatypeFilter, logic.getConfig().getDatatypeFilter());
    }

    public void assertAttributeNormalizer() {
        if (expectedNormalizers.isEmpty()) {
            return;
        }

        for (Document result : results) {
            for (String field : expectedNormalizers.keySet()) {
                Class<?> expectedNormalizer = expectedNormalizers.get(field);
                Attribute<?> attrs = result.get(field);
                if (attrs instanceof TypeAttribute<?>) {
                    TypeAttribute<?> attr = (TypeAttribute<?>) attrs;
                    assertSame(expectedNormalizer, attr.getType().getClass());
                }
            }
        }
    }

    private void assertFieldNotFound() {
        if (excludedFields.isEmpty()) {
            return;
        }

        for (Document result : results) {
            for (String field : excludedFields) {
                Attribute<?> attrs = result.get(field);
                assertNull(attrs, "Expected null value for field " + field);
            }
        }
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
        givenQuery("SHAPE == 'triangle'");
        expectPlan("SHAPE == 'triangle'");
        expectUUIDs(triangleUids);
        expectHitTermsRequiredAllOf("SHAPE:triangle");
        planAndExecuteQuery();
    }

    @Test
    public void testQuadrilaterals() throws Exception {
        givenQuery("SHAPE == 'quadrilateral'");
        expectPlan("SHAPE == 'quadrilateral'");
        expectUUIDs(quadrilateralUids);
        expectHitTermsRequiredAllOf("SHAPE:quadrilateral");
        planAndExecuteQuery();
    }

    @Test
    public void testPentagon() throws Exception {
        givenQuery("SHAPE == 'pentagon'");
        expectPlan("SHAPE == 'pentagon'");
        expectUUIDs(Set.of(ShapesIngest.pentagonUid));
        expectHitTermsRequiredAllOf("SHAPE:pentagon");
        planAndExecuteQuery();
    }

    @Test
    public void testHexagon() throws Exception {
        givenQuery("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectPlan("SHAPE == 'hexagon'");
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        planAndExecuteQuery();
    }

    @Test
    public void testOctagon() throws Exception {
        givenQuery("SHAPE == 'octagon'");
        expectPlan("SHAPE == 'octagon'");
        expectUUIDs(Set.of(ShapesIngest.octagonUid));
        expectHitTermsRequiredAllOf("SHAPE:octagon");
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilaterals() throws Exception {
        givenQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        expectPlan("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        expectUUIDs(createSet(triangleUids, quadrilateralUids));
        expectHitTermsRequiredAnyOf("SHAPE:triangle", "SHAPE:quadrilateral");
        planAndExecuteQuery();
    }

    @Test
    public void testAllShapes() throws Exception {
        givenQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectPlan("SHAPE == 'triangle' || SHAPE == 'quadrilateral' || SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectUUIDs(allUids);
        expectHitTermsRequiredAnyOf("SHAPE:triangle", "SHAPE:quadrilateral", "SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilateralsNoFilter() throws Exception {
        givenQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        expectPlan("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        expectUUIDs(createSet(triangleUids, quadrilateralUids));
        expectHitTermsRequiredAnyOf("SHAPE:triangle", "SHAPE:quadrilateral");
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilateralsCorrectFilter() throws Exception {
        givenQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        givenDatatypeFilter("triangle,quadrilateral");
        expectPlan("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        expectUUIDs(createSet(triangleUids, quadrilateralUids));
        expectHitTermsRequiredAnyOf("SHAPE:triangle", "SHAPE:quadrilateral");
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilateralsFilterForTriangles() throws Exception {
        givenQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        givenDatatypeFilter("triangle");
        expectPlan("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        expectUUIDs(triangleUids);
        expectHitTermsRequiredAllOf("SHAPE:triangle");
        planAndExecuteQuery();
    }

    @Test
    public void testTrianglesAndQuadrilateralsFilterForQuadrilaterals() throws Exception {
        givenQuery("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        givenDatatypeFilter("quadrilateral");
        expectPlan("SHAPE == 'triangle' || SHAPE == 'quadrilateral'");
        expectUUIDs(quadrilateralUids);
        expectHitTermsRequiredAllOf("SHAPE:quadrilateral");
        planAndExecuteQuery();
    }

    @Test
    public void testTrailingRegexExpansionIntoSingleTerm() throws Exception {
        givenQuery("TYPE =~ 'acu.*'");
        expectPlan("TYPE == 'acute'");
        expectUUIDs(Set.of(ShapesIngest.acuteUid));
        expectHitTermsRequiredAllOf("TYPE:acute");
        planAndExecuteQuery();
    }

    @Test
    public void testTrailingRegexExpansionIntoMultipleTerms() throws Exception {
        givenQuery("TYPE =~ 'rhomb.*'");
        expectPlan("TYPE == 'rhombus' || TYPE == 'rhomboid'");
        expectUUIDs(Set.of(ShapesIngest.rhombusUid, ShapesIngest.rhomboidUid));
        expectHitTermsRequiredAnyOf("TYPE:rhombus", "TYPE:rhomboid");
        planAndExecuteQuery();
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
        givenQuery("SHAPE =~ '.*angle'");
        expectPlan("SHAPE == 'triangle'");
        expectUUIDs(triangleUids);
        expectHitTermsRequiredAllOf("SHAPE:triangle");
        planAndExecuteQuery();
    }

    @Test
    public void testLeadingRegexExpansionIntoMultipleTerms() throws Exception {
        givenQuery("SHAPE =~ '.*gon'");
        expectPlan("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectUUIDs(otherUids);
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        planAndExecuteQuery();
    }

    @Test
    public void testLeadingRegexExpansionIntoMultipleDatatypes() throws Exception {
        givenQuery("SHAPE =~ '.*gon'");
        expectPlan("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectUUIDs(otherUids);
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        planAndExecuteQuery();
    }

    @Test
    public void testLeadingRegexExpansionIntoMultipleDatatypesWithDatatypeFilter() throws Exception {
        givenQuery("SHAPE =~ '.*gon'");
        givenDatatypeFilter("pentagon,octagon");
        expectPlan("SHAPE == 'pentagon' || SHAPE == 'octagon'");
        expectUUIDs(Set.of(ShapesIngest.pentagonUid, ShapesIngest.octagonUid));
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:octagon");
        planAndExecuteQuery();
    }

    // simple query, no filter vs. filter from params, permutations of rebuild, reduce, and prune

    @Test
    public void testSimpleQueryNoFilterSpecified() throws Exception {
        givenQuery("SHAPE == 'hexagon'");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithRebuild() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        givenQuery("SHAPE == 'hexagon'");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(allTypes);
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithReduce() throws Exception {
        logic.setReduceIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithPrune() throws Exception {
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(allTypes);
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithRebuildReduce() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setReduceIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(allTypes);
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithRebuildPrune() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        planAndExecuteQuery();
        expectDatatypeFilter(allTypes);
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithReducePrune() throws Exception {
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(allTypes);
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryNoFilterSpecifiedWithRebuildReducePrune() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(allTypes);
        planAndExecuteQuery();
    }

    // simple query with filter

    @Test
    public void testSimpleQueryFilterFromParameters() throws Exception {
        givenQuery("SHAPE == 'hexagon'");
        givenDatatypeFilter("hexagon");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithRebuild() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        givenQuery("SHAPE == 'hexagon'");
        givenDatatypeFilter("hexagon");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithReduce() throws Exception {
        logic.setReduceIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        givenDatatypeFilter("hexagon");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithPrune() throws Exception {
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        givenDatatypeFilter("hexagon");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithRebuildReduce() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setReduceIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        givenDatatypeFilter("hexagon");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithRebuildPrune() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        givenDatatypeFilter("hexagon");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithReducePrune() throws Exception {
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        givenDatatypeFilter("hexagon");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testSimpleQueryFilterFromParametersWithRebuildReducePrune() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("SHAPE == 'hexagon'");
        givenDatatypeFilter("hexagon");
        expectPlan("SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    // intersection with reduction possible

    @Test
    public void testIntersectionNoFilter() throws Exception {
        givenQuery("SHAPE == 'hexagon' && ONLY_HEX == 'hexa'");
        expectPlan("SHAPE == 'hexagon' && ONLY_HEX == 'hexa'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("SHAPE:hexagon", "ONLY_HEX:hexa");
        expectDatatypeFilter(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testFinalDatatypeFilterWhenNoneSpecified() throws Exception {
        givenQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectPlan("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectUUIDs(otherUids);
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        expectDatatypeFilter(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testFinalDatatypeFilterFromParameters() throws Exception {
        givenQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        givenDatatypeFilter("pentagon,hexagon,octagon");
        expectPlan("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectUUIDs(otherUids);
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        expectDatatypeFilter(Set.of("pentagon", "hexagon", "octagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testBuildDatatypeFilterFromQueryFields() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        givenQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectPlan("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectUUIDs(otherUids);
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        // SHAPE is common across all five datatypes
        expectDatatypeFilter(Set.of("triangle", "quadrilateral", "pentagon", "hexagon", "octagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testReduceIngestTypesWithEmptyDatatypeFilter() throws Exception {
        // this parameter will not replace an empty datatype filter
        logic.setReduceIngestTypes(true);
        givenQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectPlan("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectUUIDs(otherUids);
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        expectDatatypeFilter(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testReduceIngestTypesWithDatatypeFilterFromParametersNoChange() throws Exception {
        // SHAPE is common to five datatypes, only three specified in parameter. Reducing does not change the filter.
        logic.setReduceIngestTypes(true);
        givenQuery("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        givenDatatypeFilter("pentagon,hexagon,octagon");
        expectPlan("SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon'");
        expectUUIDs(otherUids);
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        expectDatatypeFilter(Set.of("pentagon", "hexagon", "octagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testReduceIngestTypesWithDatatypeFilterFromParameters() throws Exception {
        logic.setReduceIngestTypes(true);
        givenQuery("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        givenDatatypeFilter("pentagon,hexagon,octagon");
        // octagon datatype is pruned but the query remains intact
        expectPlan("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        expectUUIDs(Set.of(ShapesIngest.pentagonUid, ShapesIngest.hexagonUid));
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        expectHitTermsRequiredAnyOf("ONLY_PENTA:penta", "ONLY_HEX:hexa");
        expectDatatypeFilter(Set.of("pentagon", "hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testPruneIngestTypes() throws Exception {
        // octagon should be pruned given the fields unique to each datatype
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        givenDatatypeFilter("pentagon,hexagon,octagon");
        // octagon datatype is NOT pruned despite pruning the term from the query
        expectPlan("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        expectUUIDs(Set.of(ShapesIngest.pentagonUid, ShapesIngest.hexagonUid));
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        expectHitTermsRequiredAnyOf("ONLY_PENTA:penta", "ONLY_HEX:hexa");
        expectDatatypeFilter(Set.of("pentagon", "hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testReduceAndPruneIngestTypes() throws Exception {
        // octagon datatype should be pruned given the fields unique to each datatype
        logic.setReduceIngestTypes(true);
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        givenDatatypeFilter("pentagon,hexagon,octagon");
        // octagon datatype is pruned
        expectPlan("(SHAPE == 'pentagon' || SHAPE == 'hexagon' || SHAPE == 'octagon') && (ONLY_PENTA == 'penta' || ONLY_HEX == 'hexa')");
        expectUUIDs(Set.of(ShapesIngest.pentagonUid, ShapesIngest.hexagonUid));
        expectHitTermsRequiredAnyOf("SHAPE:pentagon", "SHAPE:hexagon", "SHAPE:octagon");
        expectHitTermsRequiredAnyOf("ONLY_PENTA:penta", "ONLY_HEX:hexa");
        expectDatatypeFilter(Set.of("pentagon", "hexagon"));
        planAndExecuteQuery();
    }

    // test cases for when a user specifies a filter that does not match the query fields, a filter with more types

    @Test
    public void testExclusiveFilter() {
        givenQuery("ONLY_HEX == 'hexa'");
        givenDatatypeFilter("triangle");
        expectUUIDs(Collections.emptySet());
        // datatype filter will not find ONLY_HEX and throw exception
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testExclusiveFilterWithReduce() {
        logic.setReduceIngestTypes(true);
        givenQuery("ONLY_HEX == 'hexa'");
        givenDatatypeFilter("triangle");
        expectUUIDs(Collections.emptySet());
        // datatype filter will not find ONLY_HEX and throw exception
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testExclusiveFilterWithRebuild() {
        logic.setRebuildDatatypeFilter(true);
        givenQuery("ONLY_HEX == 'hexa'");
        givenDatatypeFilter("triangle");
        expectUUIDs(Collections.emptySet());
        // datatype filter will not find ONLY_HEX and throw exception
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testExclusiveFilterWithPrune() {
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("ONLY_HEX == 'hexa'");
        givenDatatypeFilter("triangle");
        expectUUIDs(Collections.emptySet());
        // datatype filter will not find ONLY_HEX and throw exception
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testFilterWithExtraTypes() throws Exception {
        givenQuery("ONLY_HEX == 'hexa'");
        givenDatatypeFilter("triangle,quadrilateral,pentagon,hexagon,octagon");
        expectPlan("ONLY_HEX == 'hexa'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("ONLY_HEX:hexa");
        expectDatatypeFilter(allTypes);
        planAndExecuteQuery();
    }

    @Test
    public void testFilterWithExtraTypesWithReduce() throws Exception {
        logic.setReduceIngestTypes(true);
        givenQuery("ONLY_HEX == 'hexa'");
        givenDatatypeFilter("triangle,quadrilateral,pentagon,hexagon,octagon");
        expectPlan("ONLY_HEX == 'hexa'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("ONLY_HEX:hexa");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testFilterWithExtraTypesWithRebuild() throws Exception {
        logic.setRebuildDatatypeFilter(true);
        givenQuery("ONLY_HEX == 'hexa'");
        givenDatatypeFilter("triangle,quadrilateral,pentagon,hexagon,octagon");
        expectPlan("ONLY_HEX == 'hexa'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("ONLY_HEX:hexa");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testFilterWithExtraTypesWithPrune() throws Exception {
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("ONLY_HEX == 'hexa'");
        givenDatatypeFilter("triangle,quadrilateral,pentagon,hexagon,octagon");
        expectPlan("ONLY_HEX == 'hexa'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("ONLY_HEX:hexa");
        expectDatatypeFilter(Set.of("hexagon"));
        planAndExecuteQuery();
    }

    @Test
    public void testPruneNestedTermAllPermutations() throws Exception {
        // natural prune will drop the ONLY_QUAD term
        logic.setPruneQueryByIngestTypes(true);
        givenQuery("ONLY_HEX == 'hexa' && (SHAPE == 'hexagon' || ONLY_QUAD == 'square')");
        expectPlan("ONLY_HEX == 'hexa' && SHAPE == 'hexagon'");
        expectUUIDs(Set.of(ShapesIngest.hexagonUid));
        expectHitTermsRequiredAllOf("ONLY_HEX:hexa", "SHAPE:hexagon");
        planAndExecuteQuery();
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

                    givenQuery(query);
                    if (pruneOption) {
                        expectPlan(expectedPlan);
                    } else {
                        expectPlan(query);
                    }
                    expectUUIDs(Set.of(ShapesIngest.hexagonUid));
                    expectHitTermsRequiredAllOf("ONLY_HEX:hexa", "SHAPE:hexagon");
                    planAndExecuteQuery();
                }
            }
        }
    }

    @Test
    public void testSortQueryPreIndexWithImpliedCounts() throws Exception {
        try {
            // sorting via implied counts should push TYPE to the right of SHAPE
            givenQuery("TYPE == 'pentagon' || SHAPE == 'triangle'");
            givenDatatypeFilter("triangle,pentagon");
            expectPlan("SHAPE == 'triangle' || TYPE == 'pentagon'");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAnyOf("TYPE:pentagon", "SHAPE:triangle");

            disableAllSortOptions();
            logic.setSortQueryPreIndexWithImpliedCounts(true);
            planAndExecuteQuery();
        } finally {
            disableAllSortOptions();
        }
    }

    @Test
    public void testSortQueryPreIndexWithFieldCounts() throws Exception {
        try {
            // SHAPE cardinality for triangle and pentagon types is 23
            // TYPE cardinality for triangle and pentagon types is 21
            givenQuery("SHAPE == 'triangle' || TYPE == 'pentagon'");
            givenDatatypeFilter("triangle,pentagon");
            expectPlan("TYPE == 'pentagon' || SHAPE == 'triangle'");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle");

            disableAllSortOptions();
            logic.setSortQueryPreIndexWithFieldCounts(true);
            planAndExecuteQuery();
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

    @Test
    public void testLeadingRegexIvarator() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();

            givenQuery("SHAPE == 'triangle' && ((_Value_ = true) && (SHAPE =~ 'tr.*?'))");
            expectPlan("SHAPE == 'triangle' && ((_Value_ = true) && (SHAPE =~ 'tr.*?'))");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle");

            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    @Test
    public void testTrailingRegex_ContextFilter_withMatches() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();
            // TODO -- IvaratorRequired visitor needs to be more dynamic
            // before configs can be wiped out to ensure test integrity
            // disableIvaratorConfigs();

            // term cardinality is a prerequisite for regex filtering
            logic.setSortQueryPostIndexWithTermCounts(true);
            logic.setCardinalityThreshold(25);

            givenQuery("TYPE == 'acute' && ((_Value_ = true) && (SHAPE =~ 'tr.*?'))");
            expectPlan("TYPE == 'acute' && ((_Value_ = true) && (SHAPE =~ 'tr.*?'))");
            expectUUIDs(Set.of(ShapesIngest.acuteUid));
            expectHitTermsRequiredAllOf("TYPE:acute", "SHAPE:triangle");

            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    // the query is satisfiable from the field index, so the values are aggregated.
    // enabling hit list arithmetic disables this feature.
    @Test
    public void testTrailingRegex_ContextFilter_withMatches_withHitList() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();
            // TODO -- IvaratorRequired visitor needs to be more dynamic
            // before configs can be wiped out to ensure test integrity
            // disableIvaratorConfigs();

            // term cardinality is a prerequisite for regex filtering
            logic.setSortQueryPostIndexWithTermCounts(true);
            logic.setCardinalityThreshold(25);

            givenQuery("TYPE == 'acute' && ((_Value_ = true) && (SHAPE =~ 'tr.*?'))");
            expectPlan("TYPE == 'acute' && ((_Value_ = true) && (SHAPE =~ 'tr.*?'))");
            expectUUIDs(Set.of(ShapesIngest.acuteUid));
            expectHitTermsRequiredAllOf("TYPE:acute", "SHAPE:triangle");

            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    @Test
    public void testTailingRegex_ContextFilter_noMatches() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();
            // TODO -- IvaratorRequired visitor needs to be more dynamic
            // before configs can be wiped out to ensure test integrity
            // disableIvaratorConfigs();

            // term cardinality is a prerequisite for regex filtering
            logic.setSortQueryPostIndexWithTermCounts(true);
            logic.setCardinalityThreshold(25);

            // this does not intersect
            givenQuery("SHAPE == 'quadrilateral' && ((_Value_ = true) && (SHAPE =~ 'tr.*?'))");
            expectPlan("SHAPE == 'quadrilateral' && ((_Value_ = true) && (SHAPE =~ 'tr.*?'))");

            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    @Test
    public void testLeadingRegex_ContextFilter_withMatches() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();
            // TODO -- IvaratorRequired visitor needs to be more dynamic
            // before configs can be wiped out to ensure test integrity
            // disableIvaratorConfigs();

            // term cardinality is a prerequisite for regex filtering
            logic.setSortQueryPostIndexWithTermCounts(true);
            logic.setCardinalityThreshold(25);

            givenQuery("TYPE == 'equilateral' && ((_Value_ = true) && (SHAPE =~ '.*angle'))");
            expectPlan("TYPE == 'equilateral' && ((_Value_ = true) && (SHAPE =~ '.*angle'))");
            expectUUIDs(Set.of(ShapesIngest.equilateralUid));
            expectHitTermsRequiredAllOf("TYPE:equilateral", "SHAPE:triangle");

            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    @Test
    public void testLeadingRegex_ContextFilter_withMatches_withHitList() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();
            // TODO -- IvaratorRequired visitor needs to be more dynamic
            // before configs can be wiped out to ensure test integrity
            // disableIvaratorConfigs();

            // term cardinality is a prerequisite for regex filtering
            logic.setSortQueryPostIndexWithTermCounts(true);
            logic.setCardinalityThreshold(25);

            givenQuery("TYPE == 'equilateral' && ((_Value_ = true) && (SHAPE =~ '.*angle'))");
            expectPlan("TYPE == 'equilateral' && ((_Value_ = true) && (SHAPE =~ '.*angle'))");
            expectUUIDs(Set.of(ShapesIngest.equilateralUid));
            expectHitTermsRequiredAllOf("TYPE:equilateral", "SHAPE:triangle");

            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    @Test
    public void testLeadingRegex_ContextFilter_noMatches() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();
            // TODO -- IvaratorRequired visitor needs to be more dynamic
            // before configs can be wiped out to ensure test integrity
            // disableIvaratorConfigs();

            // term cardinality is a prerequisite for regex filtering
            logic.setSortQueryPostIndexWithTermCounts(true);
            logic.setCardinalityThreshold(25);

            // this does not intersect
            givenQuery("TYPE == 'quadrilateral' && ((_Value_ = true) && (SHAPE =~ '.*angle'))");
            expectPlan("TYPE == 'quadrilateral' && ((_Value_ = true) && (SHAPE =~ '.*angle'))");
            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    @Test
    public void testNestedUnionOfContextRequiredTrailingRegex() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();
            // TODO -- IvaratorRequired visitor needs to be more dynamic
            // before configs can be wiped out to ensure test integrity
            // disableIvaratorConfigs();

            // term cardinality is a prerequisite for regex filtering
            logic.setSortQueryPostIndexWithTermCounts(true);
            logic.setCardinalityThreshold(25);

            // right hand exceeded value marker does not have any backing data
            givenQuery("SHAPE == 'triangle' && (((_Value_ = true) && (SHAPE =~ 'tr.*?')) || ((_Value_ = true) && (SHAPE =~ 'zz.*?')))");
            expectPlan("SHAPE == 'triangle' && (((_Value_ = true) && (SHAPE =~ 'tr.*?')) || ((_Value_ = true) && (SHAPE =~ 'zz.*?')))");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle");

            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    @Test
    public void testBoundedRangeIvarator() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();

            givenQuery("SHAPE == 'triangle' && ((_Bounded_ = true) && (EDGES > '2' && EDGES < '7'))");
            expectPlan("SHAPE == 'triangle' && ((_Value_ = true) && ((_Bounded_ = true) && (EDGES > '+aE2' && EDGES < '+aE7')))");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle", "EDGES:3");

            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    @Test
    public void testBoundedRange_ContextFilter_withMatches() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();
            // disableIvaratorConfigs();

            // term cardinality is a prerequisite for range filtering
            logic.setSortQueryPostIndexWithTermCounts(true);
            logic.setCardinalityThreshold(25);

            givenQuery("SHAPE == 'triangle' && ((_Bounded_ = true) && (EDGES > '2' && EDGES < '7'))");
            expectPlan("SHAPE == 'triangle' && ((_Value_ = true) && ((_Bounded_ = true) && (EDGES > '+aE2' && EDGES < '+aE7')))");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle", "EDGES:3");

            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    @Test
    public void testBoundedRange_ContextFilter_noMatches() throws Exception {
        try {
            saveIndexExpansionConfigs();
            forceIvarators();
            // disableIvaratorConfigs();

            // term cardinality is a prerequisite for range filtering
            logic.setSortQueryPostIndexWithTermCounts(true);
            logic.setCardinalityThreshold(25);

            givenQuery("SHAPE == 'octagon' && ((_Bounded_ = true) && (EDGES > '2' && EDGES < '7'))");
            expectPlan("SHAPE == 'octagon' && ((_Value_ = true) && ((_Bounded_ = true) && (EDGES > '+aE2' && EDGES < '+aE7')))");
            planAndExecuteQuery();
        } finally {
            reloadIndexExpansionConfigs();
        }
    }

    /**
     * Helper method to explicitly disable ivarator configs. Ivarator configs are set at the beginning of each test, so this operation is not destructive.
     * <p>
     * Used to test context filter iterators.
     */
    private void disableIvaratorConfigs() {
        logic.setHdfsSiteConfigURLs(null);
        logic.setIvaratorCacheDirConfigs(Collections.emptyList());
    }

    private void saveIndexExpansionConfigs() {
        maxUnfieldedExpansionThreshold = logic.getMaxUnfieldedExpansionThreshold();
        maxValueExpansionThreshold = logic.getMaxValueExpansionThreshold();
    }

    private void forceIvarators() {
        logic.setMaxUnfieldedExpansionThreshold(1);
        logic.setMaxValueExpansionThreshold(1);
    }

    private void reloadIndexExpansionConfigs() {
        logic.setMaxUnfieldedExpansionThreshold(maxUnfieldedExpansionThreshold);
        logic.setMaxValueExpansionThreshold(maxValueExpansionThreshold);
    }

    @Test
    public void testAttributeNormalizers() throws Exception {
        givenQuery("SHAPE == 'triangle'");
        expectPlan("SHAPE == 'triangle'");
        expectUUIDs(triangleUids);
        expectHitTermsRequiredAllOf("SHAPE:triangle");

        expectAttributeNormalizer("EDGES", NumberType.class);
        expectAttributeNormalizer("ONLY_TRI", LcNoDiacriticsType.class);
        expectAttributeNormalizer("PROPERTIES", NoOpType.class);
        expectAttributeNormalizer("SHAPE", LcNoDiacriticsType.class);
        expectAttributeNormalizer("TYPE", LcNoDiacriticsType.class);
        expectAttributeNormalizer("UUID", NoOpType.class);
        planAndExecuteQuery();
    }

    // use projection to trigger reduction
    @Test
    public void testReduceTypeMetadataViaIncludeFields() throws Exception {
        boolean orig = logic.getReduceTypeMetadata();
        try {
            logic.setReduceTypeMetadata(true);

            givenQuery("SHAPE == 'triangle'");
            givenIncludeFields(Set.of("EDGES", "UUID", "SHAPE"));
            expectPlan("SHAPE == 'triangle'");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle");

            expectAttributeNormalizer("EDGES", NumberType.class);
            expectAttributeNormalizer("SHAPE", LcNoDiacriticsType.class);
            expectAttributeNormalizer("UUID", NoOpType.class);
            expectNoFields("ONLY_TRI", "PROPERTIES", "TYPE");
            planAndExecuteQuery();
        } finally {
            logic.setReduceTypeMetadata(orig);
        }
    }

    // use disallow listed fields to trigger reduction
    @Test
    public void testReduceTypeMetadataViaExcludeFields() throws Exception {
        boolean orig = logic.getReduceTypeMetadata();
        try {
            logic.setReduceTypeMetadata(true);

            givenQuery("SHAPE == 'triangle'");
            givenExcludeFields(Set.of("ONLY_TRI", "PROPERTIES", "TYPE"));
            expectPlan("SHAPE == 'triangle'");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle");

            expectAttributeNormalizer("EDGES", NumberType.class);
            expectAttributeNormalizer("SHAPE", LcNoDiacriticsType.class);
            expectAttributeNormalizer("UUID", NoOpType.class);
            expectNoFields("ONLY_TRI", "PROPERTIES", "TYPE");
            planAndExecuteQuery();
        } finally {
            logic.setReduceTypeMetadata(orig);
        }
    }

    // use projection to trigger reduction per shard
    @Test
    public void testReduceTypeMetadataPerShardViaIncludeFields() throws Exception {
        boolean orig = logic.getReduceTypeMetadataPerShard();
        try {
            logic.setReduceTypeMetadataPerShard(true);

            givenQuery("SHAPE == 'triangle'");
            givenIncludeFields(Set.of("EDGES", "UUID", "SHAPE"));
            expectPlan("SHAPE == 'triangle'");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle");

            expectAttributeNormalizer("EDGES", NumberType.class);
            expectAttributeNormalizer("SHAPE", LcNoDiacriticsType.class);
            expectAttributeNormalizer("UUID", NoOpType.class);
            expectNoFields("ONLY_TRI", "PROPERTIES", "TYPE");
            planAndExecuteQuery();
        } finally {
            logic.setReduceTypeMetadataPerShard(orig);
        }
    }

    // use disallow listed fields to trigger reduction
    @Test
    public void testReduceTypeMetadataPerShardViaExcludeFields() throws Exception {
        boolean orig = logic.getReduceTypeMetadataPerShard();
        try {
            logic.setReduceTypeMetadata(true);

            givenQuery("SHAPE == 'triangle'");
            givenExcludeFields(Set.of("ONLY_TRI", "PROPERTIES", "TYPE"));
            expectPlan("SHAPE == 'triangle'");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle");

            expectAttributeNormalizer("EDGES", NumberType.class);
            expectAttributeNormalizer("SHAPE", LcNoDiacriticsType.class);
            expectAttributeNormalizer("UUID", NoOpType.class);
            expectNoFields("ONLY_TRI", "PROPERTIES", "TYPE");
            planAndExecuteQuery();
        } finally {
            logic.setReduceTypeMetadata(orig);
        }
    }

    @Test
    public void testNoHitTerms() {
        if (logic.isUseDocumentScheduler()) {
            // the document scheduler always uses hit terms by default
            return;
        }
        try {
            // disabling evaluation also disables hit term generation
            logic.setDisableEvaluation(true);

            givenQuery("SHAPE == 'triangle'");
            expectPlan("SHAPE == 'triangle'");
            expectUUIDs(triangleUids);
            expectHitTermsRequiredAllOf("SHAPE:triangle");
            assertThrows(AssertionError.class, this::planAndExecuteQuery);
        } finally {
            logic.setDisableEvaluation(false);
        }
    }

    @Test
    public void testIndexExpansionTimeoutOnInitialSeek() throws Exception {
        long origTimeout = logic.getMaxIndexScanTimeMillis();
        try {
            logic.setMaxIndexScanTimeMillis(5);
            addDelayIterator(250);

            givenQuery("SHAPE =~ 'tri.*'");
            expectPlan("((_Value_ = true) && (SHAPE =~ 'tri.*'))");
            planAndExecuteQuery();
        } finally {
            logic.setMaxIndexScanTimeMillis(origTimeout);
            removeDelayIterator();
        }
    }

    @Test
    public void testIndexExpansionTimeoutOnNext() throws Exception {
        long origTimeout = logic.getMaxIndexScanTimeMillis();
        try {
            logic.setMaxIndexScanTimeMillis(300);
            addDelayIterator(250);

            givenQuery("SHAPE =~ '.*gon'");
            expectPlan("((_Value_ = true) && (SHAPE =~ '.*gon'))");
            planAndExecuteQuery();
        } finally {
            logic.setMaxIndexScanTimeMillis(origTimeout);
            removeDelayIterator();
        }
    }

    protected void addDelayIterator(int delay) {
        Set<String> names = new HashSet<>(TestIndexTableNames.names());
        names.add(TableName.SHARD_RINDEX);

        for (String indexTableName : names) {
            Map<String,String> properties = new HashMap<>();
            properties.put("table.iterator.scan.delay", "1,datawave.test.iter.DelayIterator");
            properties.put("table.iterator.scan.delay.opt.delay", String.valueOf(delay));
            MacTestUtil.addPropertiesAndWait(tops, indexTableName, properties);
        }
    }

    protected void removeDelayIterator() {
        Set<String> names = new HashSet<>(TestIndexTableNames.names());
        names.add(TableName.SHARD_RINDEX);

        for (String indexTableName : names) {
            Set<String> properties = new HashSet<>();
            properties.add("table.iterator.scan.delay");
            properties.add("table.iterator.scan.delay.opt.delay");
            MacTestUtil.removePropertiesAndWait(tops, indexTableName, properties);
        }
    }
}
