package datawave.query;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
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
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.IngestMetadata;
import datawave.test.framework.TableCreator;
import datawave.test.framework.generators.id.ModuloEventIdGenerator;
import datawave.test.framework.generators.id.SequentialEventIdGenerator;
import datawave.test.framework.generators.query.IntersectionFactory;
import datawave.test.framework.generators.query.QueryGenerator;
import datawave.test.framework.generators.query.QueryMetadata;
import datawave.test.framework.generators.query.SingleTermFactory;
import datawave.test.framework.generators.query.UnionFactory;
import datawave.test.framework.generators.query.term.EqTerm;
import datawave.test.framework.generators.value.LinearNumberGenerator;
import datawave.test.framework.generators.value.PresetGenerator;
import datawave.test.framework.writers.TableWriter;

/**
 * Classic FizzBuzz, modeled after {@link StatisticalQueryTest}.
 * <p>
 * Unlike {@link StatisticalQueryTest}, which generates its {@link FieldMetadata} combinatorically via {@link datawave.test.framework.IngestMetadataBuilder},
 * this test defines FIZZ and BUZZ {@link FieldMetadata} by hand: FIZZ appears on every event id divisible by 3, BUZZ on every event id divisible by 5, each
 * always carrying its single preset value ("fizz"/"buzz").
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
public class FizzBuzzQueryTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(FizzBuzzQueryTest.class);

    private static final int EVENT_COUNT = 100;
    private static final List<String> DATATYPES = List.of("datatype-a");

    private static AccumuloClient client;
    private static FieldMetadata idMetadata;
    private static FieldMetadata fizzMetadata;
    private static FieldMetadata buzzMetadata;
    private static QueryGenerator queryGenerator;

    private static final Authorizations auths = new Authorizations("ALL");

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(FizzBuzzQueryTest.class.getName());
        client = new InMemoryAccumuloClient("root", instance);

        idMetadata = new FieldMetadata(IngestMetadata.ID_FIELD_NAME);
        idMetadata.setMetadataColumns(List.of(I, E));
        idMetadata.setNormalizers(List.of(new LcNoDiacriticsType()));
        idMetadata.setDatatypes(DATATYPES);
        idMetadata.setEventIdGenerator(SequentialEventIdGenerator.create());
        idMetadata.setValueGenerator(LinearNumberGenerator.create());
        idMetadata.populateValues(EVENT_COUNT, EVENT_COUNT);

        fizzMetadata = new FieldMetadata("FIZZ");
        fizzMetadata.setMetadataColumns(List.of(I, E));
        fizzMetadata.setNormalizers(List.of(new LcNoDiacriticsType()));
        fizzMetadata.setDatatypes(DATATYPES);
        fizzMetadata.setEventIdGenerator(ModuloEventIdGenerator.create(3));
        fizzMetadata.setValueGenerator(PresetGenerator.of(List.of("fizz")));
        fizzMetadata.populateValues(EVENT_COUNT, 1);

        buzzMetadata = new FieldMetadata("BUZZ");
        buzzMetadata.setMetadataColumns(List.of(I, E));
        buzzMetadata.setNormalizers(List.of(new LcNoDiacriticsType()));
        buzzMetadata.setDatatypes(DATATYPES);
        buzzMetadata.setEventIdGenerator(ModuloEventIdGenerator.create(5));
        buzzMetadata.setValueGenerator(PresetGenerator.of(List.of("buzz")));
        buzzMetadata.populateValues(EVENT_COUNT, 1);

        List<FieldMetadata> fieldMetadata = List.of(idMetadata, fizzMetadata, buzzMetadata);

        // need to create and configure tables
        TableCreator.createTables(client);

        TableWriter.write(client, fieldMetadata, IngestMetadata.DEFAULT_NUM_SHARDS);

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
        assertEquals(EVENT_COUNT, queryMetadata.size());
        executeQueryMetadata(queryMetadata);
    }

    // FIZZ == 'fizz'
    @Test
    public void testSingleTermFizz() throws Exception {
        SingleTermFactory factory = new SingleTermFactory(new EqTerm());
        factory.setFieldMetadata(List.of(fizzMetadata));
        List<QueryMetadata> queryMetadata = factory.getQueries();
        assertEquals(1, queryMetadata.size());
        executeQueryMetadata(queryMetadata);
    }

    // BUZZ == 'buzz'
    @Test
    public void testSingleTermBuzz() throws Exception {
        SingleTermFactory factory = new SingleTermFactory(new EqTerm());
        factory.setFieldMetadata(List.of(buzzMetadata));
        List<QueryMetadata> queryMetadata = factory.getQueries();
        assertEquals(1, queryMetadata.size());
        executeQueryMetadata(queryMetadata);
    }

    // FIZZ == 'fizz' && BUZZ == 'buzz'
    @Test
    public void testIntersectionFizzAndBuzz() throws Exception {
        IntersectionFactory factory = new IntersectionFactory(new EqTerm(), new EqTerm());
        factory.setLeftMetadata(List.of(fizzMetadata));
        factory.setRightMetadata(List.of(buzzMetadata));
        List<QueryMetadata> queryMetadata = factory.getQueries();
        assertEquals(1, queryMetadata.size());
        executeQueryMetadata(queryMetadata);
    }

    // FIZZ == 'fizz' || BUZZ == 'buzz'
    @Test
    public void testUnionFizzAndBuzz() throws Exception {
        UnionFactory factory = new UnionFactory(new EqTerm(), new EqTerm());
        factory.setLeftMetadata(List.of(fizzMetadata));
        factory.setRightMetadata(List.of(buzzMetadata));
        List<QueryMetadata> queryMetadata = factory.getQueries();
        assertEquals(1, queryMetadata.size());
        executeQueryMetadata(queryMetadata);
    }

    protected void executeQueryMetadata(List<QueryMetadata> queryMetadata) throws Exception {
        for (QueryMetadata metadata : queryMetadata) {
            givenDate("20260202");
            givenQuery(metadata.getQuery());
            expectPlan(metadata.getPlan());
            expectResultCount(metadata.getIds().size());
            if (log.isDebugEnabled()) {
                log.debug("query: {}", metadata.getQuery());
                log.debug(" plan: {}", metadata.getPlan());
            }
            planAndExecuteQuery();
            // Multiple queries run within a single @Test method here, so teardown must happen once per iteration.
            // JUnit's @AfterEach only fires once the test method returns, which is too late to reset state between queries.
            afterEach();
        }
    }
}
