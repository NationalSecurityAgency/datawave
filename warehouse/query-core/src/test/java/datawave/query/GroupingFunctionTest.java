package datawave.query;

import java.text.ParseException;
import java.util.TimeZone;

import datawave.query.util.WiseGuysIngest;
import datawave.test.MacTestUtil;
import datawave.util.TableName;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
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
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.NoExpansionTests;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.VisibilityWiseGuysIngestWithModel;

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
public class GroupingFunctionTest extends AbstractQueryTest {
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient clientForSetup;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(GroupingFunctionTest.class.getName());
        clientForSetup = new InMemoryAccumuloClient("", instance);

        TableOperations tops = clientForSetup.tableOperations();
        MacTestUtil.createOrRecreate(tops, TableName.METADATA);
        MacTestUtil.createOrRecreate(tops, TableName.SHARD);
        MacTestUtil.createOrRecreate(tops, TableName.SHARD_INDEX);
        MacTestUtil.createOrRecreate(tops, TableName.SHARD_RINDEX);

        // this helper class will generate the extra index tables
        WiseGuysIngest.writeItAll(clientForSetup, WiseGuysIngest.WhatKindaRange.SHARD);
    }

    @BeforeEach
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        givenDate("20091231", "20150101");

        setClientForTest(clientForSetup);
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

    }

    @Override
    protected void extraAssertions() {

    }

    @Test
    public void baseTest() throws Exception {
        givenQuery("UUID == 'corleone' && PHILOSOPHY == 'prioritized' && PHILOSOPHY == 'power'");
        expectPlan("UUID == 'corleone' && PHILOSOPHY == 'prioritized' && PHILOSOPHY == 'power'");
        expectResultCount(1);
        planAndExecuteQuery();
    }

    @Test
    public void testMe() throws Exception {
        givenQuery("UUID == 'corleone' && grouping:matchesInGroup(PHILOSOPHY,'alt1', PHILOSOPHY, 'alt2')");
        expectPlan("UUID == 'corleone' && grouping:matchesInGroup(PHILOSOPHY,'alt1', PHILOSOPHY, 'alt2')");
        expectResultCount(1);
        planAndExecuteQuery();
    }
}
