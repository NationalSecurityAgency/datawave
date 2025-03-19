package datawave.query.tables.term;

import static datawave.query.util.WiseGuysIngest.corleoneUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang3.StringUtils;
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

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.config.TermFrequencyQueryConfiguration;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.ShardQueryLogicTest;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.transformer.TermFrequencyQueryTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

public abstract class TermFrequencyQueryTableTest {
    private static final Logger log = Logger.getLogger(TermFrequencyQueryTableTest.class);

    private static final Authorizations auths = new Authorizations("ALL");
    private static final Set<Authorizations> authSet = Collections.singleton(auths);

    @Inject
    @SpringBean(name = "TermFrequencyQuery")
    protected TermFrequencyQueryTable logic;
    protected KryoDocumentDeserializer deserializer;

    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private final Map<String,String> queryParameters = new HashMap<>();

    private String query;

    protected abstract String getRange();

    @RunWith(Arquillian.class)
    public static class ShardRange extends TermFrequencyQueryTableTest {

        @Override
        protected String getRange() {
            return WiseGuysIngest.WhatKindaRange.SHARD.name();
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends TermFrequencyQueryTableTest {

        @Override
        protected String getRange() {
            return WiseGuysIngest.WhatKindaRange.DOCUMENT.name();
        }
    }

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        TypeRegistry.reset();
    }

    @Before
    public void setup() {
        this.deserializer = new KryoDocumentDeserializer();
    }

    @After
    public void tearDown() {
        this.logic = null;
    }

    private AccumuloClient createClient() throws Exception {
        AccumuloClient client = new QueryTestTableHelper(ShardQueryLogicTest.ShardRange.class.toString(), log,
                        RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER).client;
        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.valueOf(getRange()));
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        return client;
    }

    private Query createSettings() {
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(this.query);
        settings.setParameters(this.queryParameters);
        settings.setId(UUID.randomUUID());
        return settings;
    }

    @Test
    public void injectionTest() {
        assertNotNull(logic);
    }

    @Test
    public void corleoneTest() throws Exception {
        String shard = "20130101_0";
        String dataType = "test";
        String uid = corleoneUID;

        this.query = shard + "/" + dataType + "/" + uid;

        List<String> expectedTfs = new ArrayList<>();

        expectedTfs.add("QUOTE,,an,1,[4]");
        expectedTfs.add("QUOTE,,cant,1,[7]");
        expectedTfs.add("QUOTE,,gonna,1,[1]");
        expectedTfs.add("QUOTE,,he,1,[6]");
        expectedTfs.add("QUOTE,,him,1,[3]");
        expectedTfs.add("QUOTE,hash1,i,1,[0]");
        expectedTfs.add("QUOTE,,im,1,[0]");
        expectedTfs.add("QUOTE,,make,1,[2]");
        expectedTfs.add("QUOTE,hash1,never,1,[1]");
        expectedTfs.add("QUOTE,,offer,1,[5]");
        expectedTfs.add("QUOTE,,refuse,1,[8]");
        expectedTfs.add("QUOTE,hash1,refuse,1,[2]");

        runQuery(shard, dataType, uid, "QUOTE", expectedTfs);
    }

    private void runQuery(String shard, String dataType, String uid, String fieldLimit, List<String> expectedTfs) throws Exception {
        AccumuloClient client = createClient();
        Query settings = createSettings();
        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);

        Range expected = new Range(new Key(shard, "tf", dataType + '\u0000' + uid + '\u0000'), true, new Key(shard, "tf", dataType + '\u0000' + uid + "\1"),
                        false);

        assertEquals(expected, ((TermFrequencyQueryConfiguration) config).getRange());

        logic.setupQuery(config);

        TermFrequencyQueryTransformer transformer = (TermFrequencyQueryTransformer) (logic.getTransformer(settings));
        TransformIterator iter = new DatawaveTransformIterator(logic.iterator(), transformer);
        List<Object> eventList = new ArrayList<>();
        while (iter.hasNext()) {
            eventList.add(iter.next());
        }

        BaseQueryResponse response = transformer.createResponse(eventList);
        assertTrue(response instanceof DefaultEventQueryResponse);
        DefaultEventQueryResponse eventQueryResponse = (DefaultEventQueryResponse) response;

        for (EventBase event : eventQueryResponse.getEvents()) {
            String name = null;
            String extendedName = null;
            String value = null;
            String count = null;
            String offsets = null;

            for (DefaultField eventField : (List<DefaultField>) event.getFields()) {
                switch (eventField.getName()) {
                    case "FIELD_NAME":
                        name = eventField.getValueString();
                        break;
                    case "FIELD_VALUE":
                        value = eventField.getValueString();
                        break;
                    case "OFFSET_COUNT":
                        count = eventField.getValueString();
                        break;
                    case "OFFSETS":
                        offsets = eventField.getValueString();
                        break;
                    case "EXTENDED_FIELD_NAME":
                        extendedName = eventField.getValueString();
                        break;
                }
            }
            String tfComposite = StringUtils.join(new String[] {name, extendedName, value, count, offsets}, ",");
            if (fieldLimit != null && fieldLimit.equals(name)) {
                assertTrue("unexpected tf: " + tfComposite, expectedTfs.remove(tfComposite));
            }
        }
    }
}
