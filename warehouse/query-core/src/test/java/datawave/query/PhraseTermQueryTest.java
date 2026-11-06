package datawave.query;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.TableCreator;
import datawave.test.framework.generators.query.term.PhraseTerm;
import datawave.test.framework.util.MetadataColumn;
import datawave.test.framework.util.ShardKeyUtil;
import datawave.test.framework.writers.TableWriter;

@ExtendWith(SpringExtension.class)
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
class PhraseTermQueryTest extends AbstractQueryTest {

    private static final Authorizations AUTHS = new Authorizations(TableWriter.DEFAULT_VISIBILITY);
    private static final List<String> VALUES = List.of("alpha beta", "alpha gamma", "zero Alpha Béta end");
    private static AccumuloClient client;

    @Autowired
    @Qualifier("EventQuery")
    private ShardQueryLogic configuredLogic;

    private ShardQueryLogic logic;

    @BeforeAll
    static void createData() throws Exception {
        client = new InMemoryAccumuloClient("root", new InMemoryInstance(PhraseTermQueryTest.class.getSimpleName() + "-" + UUID.randomUUID()));
        client.securityOperations().changeUserAuthorizations("root", AUTHS);
        TableCreator.createTables(client);
        TableWriter.write(client, List.of(field("UUID", List.of("one", "two", "three"), I, E), field("ANCHOR", List.of("all"), I, E),
                        field("INDEXED", VALUES, I, E, TF), field("INDEX_ONLY", VALUES, I, TF), field("EVENT_ONLY", VALUES, E, TF)), 10);
    }

    @AfterAll
    static void closeClient() {
        client.close();
    }

    @BeforeEach
    void setup() {
        logic = new ShardQueryLogic(configuredLogic);
        setClientForTest(client);
    }

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return AUTHS;
    }

    @Override
    protected List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX);
    }

    @Override
    protected void extraConfigurations() {
        logic.setFullTableScanEnabled(false);
    }

    @Override
    protected void extraAssertions() {}

    private static Stream<Arguments> contentFields() {
        return Stream.of(Arguments.of(field("INDEXED", VALUES, I, E, TF), ""), Arguments.of(field("INDEX_ONLY", VALUES, I, TF), ""),
                        Arguments.of(field("EVENT_ONLY", VALUES, E, TF), "ANCHOR == 'all' && "));
    }

    @ParameterizedTest
    @MethodSource("contentFields")
    void testPhraseAcrossContentFieldLayouts(FieldMetadata field, String anchor) throws Exception {
        PhraseTerm term = new PhraseTerm();
        List<String> queries = new ArrayList<>();
        List<String> plans = new ArrayList<>();
        List<List<Integer>> eventIds = new ArrayList<>();
        term.forEachEvaluation(field, () -> {
            queries.add(term.getQueryTerm());
            plans.add(term.getPlanTerm());
            eventIds.add(term.getEventIds());
        });
        assertEquals(3, queries.size());
        assertEquals(List.of(1, 3), eventIds.get(0));

        givenDate(ShardKeyUtil.DATE);
        givenQuery(anchor + queries.get(0));
        expectPlan(anchor + plans.get(0));
        expectResultCount(2);
        expectUUIDs(Set.of("one", "three"));
        planAndExecuteQuery();
    }

    private static FieldMetadata field(String name, List<String> values, MetadataColumn... columns) {
        FieldMetadata field = new FieldMetadata(name);
        field.setMetadataColumns(List.of(columns));
        field.setNormalizers(List.of(new LcNoDiacriticsType()));
        field.setDatatypes(List.of("test"));
        field.setValues(values);
        field.setEventIds(List.of(1, 2, 3));
        return field;
    }
}
