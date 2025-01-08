package datawave.query;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.helpers.PrintUtility;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

@RunWith(SpringJUnit4ClassRunner.class)
// should mirror what is in beanRefContext.xml
@ContextConfiguration(locations = {"/datawave/query/QueryLogicFactory.xml", "/MarkingFunctionsContext.xml", "/MetadataHelperContext.xml",
        "/JexlFunctionNamespaceRegistryContext.xml", "/CacheContext.xml"})
// recreate the context after every test
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class BaseWiseGuyTest {
    private static final Logger log = Logger.getLogger(BaseWiseGuyTest.class);
    private static Authorizations auths = new Authorizations("ALL");

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private static AccumuloClient client;

    @BeforeClass
    public static void setupAccumulo() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(UniqueTest.ShardRange.class.toString(), log,
                        RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
        client = qtth.client;

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @Test
    public void testAutowire() {
        assertNotNull(logic);
    }

    protected Date getDate(String date) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd hhmmss");
        return sdf.parse(date);
    }

    protected Query getSettings(String query, Date startDate, Date endDate) {
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(query);
        settings.setId(UUID.randomUUID());

        return settings;
    }

    protected void runQuery(Query settings, Set<String> expected) throws Exception {
        GenericQueryConfiguration config = logic.initialize(client, settings, Set.of(auths));
        logic.setupQuery(config);

        DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(settings));
        TransformIterator iter = new DatawaveTransformIterator(logic.iterator(), transformer);
        List<Object> eventList = new ArrayList<>();
        while (iter.hasNext()) {
            eventList.add(iter.next());
        }

        BaseQueryResponse response = transformer.createResponse(eventList);

        Assert.assertTrue(response instanceof DefaultEventQueryResponse);
        DefaultEventQueryResponse eventQueryResponse = (DefaultEventQueryResponse) response;

        if (eventQueryResponse.getReturnedEvents() > 0) {
            for (EventBase event : eventQueryResponse.getEvents()) {
                boolean found = expected.remove(event.getMetadata().getInternalId());
                Assert.assertTrue("expected event " + event.getMetadata().getInternalId() + " not found", found);
            }
            Assert.assertTrue(expected.isEmpty());
        } else {
            // no results in the response
            assertTrue(expected.isEmpty());
        }
    }
}
