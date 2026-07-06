package datawave.query;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.apache.log4j.helpers.Loader.getResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.PropertyConfigurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.IngestMetadata;
import datawave.test.framework.IngestMetadataBuilder;
import datawave.test.framework.TableCreator;
import datawave.test.framework.generators.query.QueryGenerator;
import datawave.test.framework.generators.query.QueryMetadata;
import datawave.test.framework.generators.query.term.EqTerm;
import datawave.test.framework.generators.query.term.IsNotNullTerm;
import datawave.test.framework.generators.query.term.IsNullTerm;
import datawave.test.framework.generators.query.term.NeTerm;
import datawave.test.framework.generators.query.term.PhraseTerm;
import datawave.test.framework.writers.TableWriter;

/**
 * Test that uses the {@link datawave.test.framework.IngestMetadata}
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class StatisticalQueryTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(StatisticalQueryTest.class);

    private static AccumuloClient client;
    private static IngestMetadata metadata;
    private static QueryGenerator queryGenerator;

    private static final Authorizations auths = new Authorizations("ALL");

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    /**
     * Query counts are derived from {@link QueryGenerator}
     */
    private static final String ASSERTION_MESSAGE = "This assertion ensures the operator knows how many queries are run";

    @BeforeAll
    public static void beforeAll() throws Exception {
        // log4j config suppresses noisy messages from query planning.
        PropertyConfigurator.configure(getResource("log4j-stats.properties"));

        InMemoryInstance instance = new InMemoryInstance(StatisticalQueryTest.class.getName());
        client = new InMemoryAccumuloClient("root", instance);

        //  @formatter:off
        metadata = IngestMetadataBuilder.builder()
                .setMetadataColumns(List.of(I, E, TF))
                .addNormalizers(List.of(new LcNoDiacriticsType(), new NumberType()))
                .enableAlphabeticFields()
                .enableNumericFields()
                .build();
        //  @formatter:on

        metadata.create(75);
        List<FieldMetadata> fieldMetadata = metadata.getFieldMetadata();

        // need to create and configure tables
        TableCreator.createTables(client);

        TableWriter.write(client, fieldMetadata, metadata.getNumShards());

        client.securityOperations().changeUserAuthorizations("root", auths);

        queryGenerator = QueryGenerator.create(fieldMetadata);
    }

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
        // no-op for now
    }

    @Override
    protected void extraAssertions() {
        // no-op for now
    }

    @Override
    protected List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX);
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        setClientForTest(client);
        givenDate("20260202");
    }

    // ID == 'value'
    @Test
    public void testQueryByIdField() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.singleTermId(new EqTerm()).getQueries();
        assertEquals(metadata.getEventCount(), queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // ---- Single ----

    // INDEXED == 'value'
    @Test
    public void testSingleOfIndexedEq() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.singleTermIndexed(new EqTerm()).getQueries();
        assertEquals(48, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEX_ONLY == 'value'
    @Test
    public void testSingleOfIndexOnlyEq() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.singleTermIndexOnly(new EqTerm()).getQueries();
        assertEquals(24, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // ---- Content ----

    // content:phrase(TOKENIZED_INDEX_ONLY, termOffsetMap, 'w1', 'w2')
    @Test
    public void testSingleOfTokenizedIndexOnlyPhrase() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.singleTermTokenizedIndexOnly(new PhraseTerm()).getQueries();
        assertEquals(8, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEXED == 'value' && content:phrase(TOKENIZED_EVENT_ONLY, termOffsetMap, 'w1', 'w2')
    @Test
    public void testIntersectionOfIndexedEqAndTokenizedEventOnlyPhrase() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexedAndTokenizedEventOnly(new EqTerm(), new PhraseTerm()).getQueries();
        assertEquals(384, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // ---- Union, self-composed ----

    // INDEXED == 'value1' || INDEXED == 'value2'
    @Test
    public void testUnionOfIndexedEqAndIndexedEq() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.unionIndexedTerms(new EqTerm(), new EqTerm()).getQueries();
        assertEquals(1104, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEX_ONLY == 'value1' || INDEX_ONLY == 'value2'
    @Test
    public void testUnionOfIndexOnlyEqAndIndexOnlyEq() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.unionIndexOnlyTerms(new EqTerm(), new EqTerm()).getQueries();
        assertEquals(264, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // ---- Intersection, indexed + indexed, self-composed ----

    // INDEXED == 'value1' && INDEXED == 'value2'
    @Test
    public void testIntersectionOfIndexedEqAndIndexedEq() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexedTerms(new EqTerm(), new EqTerm()).getQueries();
        assertEquals(1104, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEXED == 'value1' && !(INDEXED == 'value2')
    @Test
    public void testIntersectionOfIndexedEqAndIndexedNe() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexedTerms(new EqTerm(), new NeTerm()).getQueries();
        assertEquals(1104, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // ---- Intersection, indexOnly + indexOnly, self-composed ----

    // INDEX_ONLY == 'value1' && INDEX_ONLY == 'value2'
    @Test
    public void testIntersectionOfIndexOnlyEqAndIndexOnlyEq() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexOnlyTerms(new EqTerm(), new EqTerm()).getQueries();
        assertEquals(264, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEX_ONLY == 'value1' && !(INDEX_ONLY == 'value2')
    @Test
    public void testIntersectionOfIndexOnlyEqAndIndexOnlyNe() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexOnlyTerms(new EqTerm(), new NeTerm()).getQueries();
        assertEquals(264, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // ---- Intersection, indexed + eventOnly ----

    // INDEXED == 'value' && EVENT_ONLY == 'value'
    @Test
    public void testIntersectionOfIndexedEqAndEventOnlyEq() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexedAndEventOnly(new EqTerm(), new EqTerm()).getQueries();
        assertEquals(1152, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEXED == 'value' && !(EVENT_ONLY == 'value')
    @Test
    public void testIntersectionOfIndexedEqAndEventOnlyNe() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexedAndEventOnly(new EqTerm(), new NeTerm()).getQueries();
        assertEquals(1152, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEXED == 'value' && filter:isNotNull(EVENT_ONLY)
    @Test
    public void testIntersectionOfIndexedEqAndEventOnlyIsNotNull() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexedAndEventOnly(new EqTerm(), new IsNotNullTerm()).getQueries();
        assertEquals(576, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEXED == 'value' && filter:isNull(EVENT_ONLY)
    @Test
    public void testIntersectionOfIndexedEqAndEventOnlyIsNull() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexedAndEventOnly(new EqTerm(), new IsNullTerm()).getQueries();
        assertEquals(576, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // ---- Intersection, indexOnly + eventOnly ----

    // INDEX_ONLY == 'value' && EVENT_ONLY == 'value'
    @Test
    public void testIntersectionOfIndexOnlyEqAndEventOnlyEq() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexOnlyAndEventOnly(new EqTerm(), new EqTerm()).getQueries();
        assertEquals(576, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEX_ONLY == 'value' && !(EVENT_ONLY == 'value')
    @Test
    public void testIntersectionOfIndexOnlyEqAndEventOnlyNe() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexOnlyAndEventOnly(new EqTerm(), new NeTerm()).getQueries();
        assertEquals(576, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEX_ONLY == 'value' && filter:isNotNull(EVENT_ONLY)
    @Test
    public void testIntersectionOfIndexOnlyEqAndEventOnlyIsNotNull() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexOnlyAndEventOnly(new EqTerm(), new IsNotNullTerm()).getQueries();
        assertEquals(288, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    // INDEX_ONLY == 'value' && filter:isNull(EVENT_ONLY)
    @Test
    public void testIntersectionOfIndexOnlyEqAndEventOnlyIsNull() throws Exception {
        List<QueryMetadata> queryMetadata = queryGenerator.intersectionIndexOnlyAndEventOnly(new EqTerm(), new IsNullTerm()).getQueries();
        assertEquals(288, queryMetadata.size(), ASSERTION_MESSAGE);
        executeQueryMetadata(queryMetadata);
    }

    protected void executeQueryMetadata(List<QueryMetadata> queryMetadata) throws Exception {
        for (QueryMetadata metadata : queryMetadata) {
            givenDate("20260202");
            givenQuery(metadata.getQuery());
            expectPlan(metadata.getPlan());
            expectResultCount(metadata.getIds().size());
            // log.info("query: {}", metadata.getQuery());
            // log.info(" plan: {}", metadata.getPlan());
            planAndExecuteQuery();
            // clean after each query
            afterEach();
        }
    }
}
