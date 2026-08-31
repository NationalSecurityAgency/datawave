package datawave.query.tables.term;

import static datawave.query.util.WiseGuysIngest.corleoneUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.lang3.StringUtils;
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

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.helpers.PrintUtility;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.config.TermFrequencyQueryConfiguration;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.transformer.TermFrequencyQueryTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

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
public class TermFrequencyQueryTableTest {

    private static final Logger log = Logger.getLogger(TermFrequencyQueryTableTest.class);

    private static final Authorizations auths = new Authorizations("ALL");
    private static final Set<Authorizations> authSet = Collections.singleton(auths);

    private static AccumuloClient client;

    @Autowired
    @Qualifier("TermFrequencyQuery")
    protected TermFrequencyQueryTable logic;
    protected KryoDocumentDeserializer deserializer;

    private final Map<String,String> queryParameters = new HashMap<>();

    private String query;

    @BeforeAll
    public static void beforeAll() throws Exception {
        // WhatKindaRange only affects how UIDs are encoded in the global index, which this
        // query logic never scans, so a single ingest is sufficient here.
        client = new QueryTestTableHelper(TermFrequencyQueryTableTest.class.toString(), log, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY,
                        RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER).client;
        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @BeforeEach
    public void setup() {
        this.deserializer = new KryoDocumentDeserializer();
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
                assertTrue(expectedTfs.remove(tfComposite), "unexpected tf: " + tfComposite);
            }
        }
    }
}
