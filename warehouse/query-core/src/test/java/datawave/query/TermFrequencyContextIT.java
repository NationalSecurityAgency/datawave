package datawave.query;

import java.nio.file.Path;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
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

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcType;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractIngest;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = {"datawave.configuration.spring", "datawave.query"})
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class TermFrequencyContextIT extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(TermFrequencyContextIT.class);

    @TempDir
    public static Path folder;

    private static final Authorizations auths = new Authorizations("ALL");

    protected static AccumuloClient client = null;

    protected static AbstractIngest ingest = null;

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
        // no-op
    }

    @Override
    protected List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {

        InMemoryInstance i = new InMemoryInstance(TermFrequencyContextIT.class.getName());
        client = new InMemoryAccumuloClient("", i);

        ingest = new AbstractIngest(client, auths);
        ingest.registerField("TF", new LcType());
        ingest.registerColumns("TF", List.of("i", "tf"));

        ingest.registerField("ID", new LcType());
        ingest.registerColumns("ID", List.of("i", "e"));

        // mark TOK (and only TOK) as a content-expansion field, i.e. a field eligible for unfielded content:phrase() queries.
        // this mirrors real deployments where content expansion fields are explicitly curated, so TF is *not* a content
        // expansion field. this matters because TermOffsetPopulator only writes a TF-column entry with
        // contentExpansionField=true when the field is (or is unconfigured and therefore defaults to) a content expansion
        // field; otherwise it writes contentExpansionField=false.
        ingest.registerField("TOK", new LcType());
        ingest.registerColumns("TOK", List.of("content"));

        // simple event
        ingest.writeFV(1, "ID", "1");

        // simple phrase
        ingest.writeFV(2, "ID", "2");
        ingest.writeTokenized(2, "TF", "see spot run");

        // phrase with context
        ingest.writeFV(3, "ID", "3");
        ingest.writeTokenized(3, "TF.123", "we go to the park");
        ingest.writeTokenized(3, "TF.456", "we go to the ocean");
        ingest.writeTokenized(3, "TF.789", "we go to the playground");
    }

    @BeforeEach
    public void beforeEach() {
        setClientForTest(client);
    }

    @Test
    public void printTables() {
        ingest.printTable(TableName.METADATA);
        ingest.printTable(TableName.SHARD_INDEX, ingest.uid(3));
        ingest.printTable(TableName.SHARD, ingest.uid(3));
    }

    @Test
    public void testSimpleIdQuery() throws Exception {
        givenDate(ingest.getDate());
        givenQuery("ID == '1'");
        expectPlan("ID == '1'");
        expectResultCount(1);
        planAndExecuteQuery();
    }

    @Test
    public void testSeeSpotRun() throws Exception {
        givenDate(ingest.getDate());
        givenQuery("content:phrase(TF, termOffsetMap, 'spot', 'run')");
        expectPlan("content:phrase(TF, termOffsetMap, 'spot', 'run') && TF == 'spot' && TF == 'run'");
        expectResultCount(1);
        planAndExecuteQuery();
    }

    @Test
    public void testSpotWithContext() throws Exception {
        givenDate(ingest.getDate());
        givenQuery("content:phrase(TF, termOffsetMap, 'spot', 'run')");
        expectPlan("content:phrase(TF, termOffsetMap, 'spot', 'run') && TF == 'spot' && TF == 'run'");
        expectResultCount(1);
        planAndExecuteQuery();
    }

    @Test
    public void testToThePark() throws Exception {
        givenDate(ingest.getDate());
        givenQuery("content:phrase(TF, termOffsetMap, 'to', 'the', 'park')");
        expectPlan("content:phrase(TF, termOffsetMap, 'to', 'the', 'park') && TF == 'to' && TF == 'the' && TF == 'park'");
        expectResultCount(1);
        planAndExecuteQuery();
    }

    @Test
    public void testToTheOcean() throws Exception {
        givenDate(ingest.getDate());
        givenQuery("content:phrase(TF, termOffsetMap, 'to', 'the', 'ocean')");
        expectPlan("content:phrase(TF, termOffsetMap, 'to', 'the', 'ocean') && TF == 'to' && TF == 'the' && TF == 'ocean'");
        expectResultCount(1);
        planAndExecuteQuery();
    }

    @Test
    public void testToThePlayGround() throws Exception {
        givenDate(ingest.getDate());
        givenQuery("content:phrase(TF, termOffsetMap, 'to', 'the', 'playground')");
        expectPlan("content:phrase(TF, termOffsetMap, 'to', 'the', 'playground') && TF == 'to' && TF == 'the' && TF == 'playground'");
        expectResultCount(1);
        planAndExecuteQuery();
    }
}
