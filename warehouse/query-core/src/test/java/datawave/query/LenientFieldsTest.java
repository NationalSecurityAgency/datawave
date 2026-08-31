package datawave.query;

import java.util.Collections;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;

/**
 * Loads some data in a mock accumulo table and then issues queries against the table using the shard query table.
 *
 */
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
public class LenientFieldsTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(LenientFieldsTest.class);

    private static final Authorizations auths = new Authorizations("ALL");

    protected Set<Authorizations> authSet = Collections.singleton(auths);

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    protected static AccumuloClient client = null;

    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(LenientFieldsTest.class.getName());
        client = new InMemoryAccumuloClient("root", instance);

        QueryTestTableHelper tableHelper = new QueryTestTableHelper(client, log);
        tableHelper.configureTables(new MockAccumuloRecordWriter());

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        ModelAdditions.addLenientModelEntries(client);

        IndexIngestUtil util = new IndexIngestUtil();
        util.write(client, auths);
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
        givenDate("20091231", "20150101");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.LENIENT_FIELDS, "ETA,AGE,MAGIC,NOME,NAME,NAM,AG");
        getLogic().setFullTableScanEnabled(true);
    }

    @Override
    protected void extraAssertions() {

    }

    private static class ModelAdditions extends WiseGuysIngest {
        private static void addLenientModelEntries(AccumuloClient client) throws TableNotFoundException, MutationsRejectedException {
            BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);

            try (BatchWriter bw = client.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig)) {
                Mutation mutation = new Mutation("NAM");
                mutation.put("DATAWAVE", "MAGIC" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
                mutation.put("DATAWAVE", "lenient" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
                bw.addMutation(mutation);

                mutation = new Mutation("AG");
                mutation.put("DATAWAVE", "lenient" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
                bw.addMutation(mutation);
            }
        }
    }

    @BeforeEach
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        setClientForTest(client);
    }

    @Test
    public void testGreaterThanAlphaNumeric() throws Exception {
        givenQuery("AG > 'abc10'");
        expectPlan("(((_Drop_ = true) && ((_Reason_ = 'Normalizations failed and not strict') && (_Query_ = 'ETA > \\'abc10\\''))) || ((_Drop_ = true) && ((_Reason_ = 'Normalizations failed and not strict') && (_Query_ = 'AGE > \\'abc10\\''))))");
        expectResultCount(0);
        planAndExecuteQuery();
    }

    @Test
    public void testEqualsNumeric() throws Exception {
        givenQuery("AG == '40'");
        expectPlan("(ETA == '+bE4' || AGE == '+bE4')");
        expectUUIDs(Set.of("CORLEONE", "CAPONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testGreaterThanNumeric() throws Exception {
        givenQuery("NAM > '40'");
        expectPlan("(MAGIC > '+bE4' || NOME > '40' || NAME > '40')");
        expectUUIDs(Set.of("CORLEONE", "SOPRANO", "CAPONE", "TATTAGLIA"));
        planAndExecuteQuery();
    }

    @Test
    public void testEqualsAlphaNumeric() throws Exception {
        givenQuery("NAM == 'abc40'");
        expectPlan("(NAME == 'abc40' || NOME == 'abc40' || ((_Drop_ = true) && ((_Reason_ = 'Normalizations failed and not strict') && (_Query_ = 'MAGIC == \\'abc40\\''))))");
        expectResultCount(0);
        planAndExecuteQuery();
    }
}
