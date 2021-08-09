package datawave.query.transformer;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import datawave.configuration.spring.SpringBean;
import datawave.data.type.LcType;
import datawave.data.type.NumberType;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.marking.MarkingFunctions;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.attributes.Attribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.language.parser.jexl.JexlControlledQueryParser;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.GroupingTransform.GroupingTypeAttribute;
import datawave.query.util.VisibilityWiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.iterator.DatawaveTransformIterator;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import static datawave.query.RebuildingScannerTestHelper.TEARDOWN.ALWAYS;
import static datawave.query.RebuildingScannerTestHelper.TEARDOWN.ALWAYS_SANS_CONSISTENCY;
import static datawave.query.RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER;
import static datawave.query.RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY;
import static datawave.query.RebuildingScannerTestHelper.TEARDOWN.NEVER;
import static datawave.query.RebuildingScannerTestHelper.TEARDOWN.RANDOM;
import static datawave.query.RebuildingScannerTestHelper.TEARDOWN.RANDOM_SANS_CONSISTENCY;

/**
 * Applies grouping to queries
 * 
 */
public abstract class GroupingTest {
    
    private static final Logger log = Logger.getLogger(GroupingTest.class);
    
    private static Authorizations auths = new Authorizations("ALL", "E", "I");
    
    // @formatter:off
    private static final List<RebuildingScannerTestHelper.TEARDOWN> TEARDOWNS = Lists.newArrayList(
            NEVER,
            ALWAYS,
            ALWAYS_SANS_CONSISTENCY,
            RANDOM,
            RANDOM_SANS_CONSISTENCY,
            EVERY_OTHER,
            EVERY_OTHER_SANS_CONSISTENCY
    );
    private static final List<RebuildingScannerTestHelper.INTERRUPT> INTERRUPTS = Arrays.asList(RebuildingScannerTestHelper.INTERRUPT.values());
    // @formatter:on
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends GroupingTest {
        
        @Override
        protected BaseQueryResponse runTestQueryWithGrouping(Map<String,Integer> expected, String querystr, Date startDate, Date endDate,
                        Map<String,String> extraParms, RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt)
                        throws Exception {
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.getName(), log, teardown, interrupt);
            Connector connector = qtth.connector;
            VisibilityWiseGuysIngest.writeItAll(connector, VisibilityWiseGuysIngest.WhatKindaRange.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
            return super.runTestQueryWithGrouping(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends GroupingTest {
        
        @Override
        protected BaseQueryResponse runTestQueryWithGrouping(Map<String,Integer> expected, String querystr, Date startDate, Date endDate,
                        Map<String,String> extraParms, RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt)
                        throws Exception {
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log, teardown, interrupt);
            Connector connector = qtth.connector;
            VisibilityWiseGuysIngest.writeItAll(connector, VisibilityWiseGuysIngest.WhatKindaRange.DOCUMENT);
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
            return super.runTestQueryWithGrouping(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    
    protected KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @Deployment
    public static JavaArchive createDeployment() {
        
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }
    
    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        logic.setFullTableScanEnabled(true);
        logic.setMaxEvaluationPipelines(1);
        deserializer = new KryoDocumentDeserializer();
    }
    
    protected abstract BaseQueryResponse runTestQueryWithGrouping(Map<String,Integer> expected, String querystr, Date startDate, Date endDate,
                    Map<String,String> extraParms, RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt)
                    throws Exception;
    
    protected BaseQueryResponse runTestQueryWithGrouping(Map<String,Integer> expected, String querystr, Date startDate, Date endDate,
                    Map<String,String> extraParms, Connector connector) throws Exception {
        log.debug("runTestQueryWithGrouping");
        
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(querystr);
        settings.setParameters(extraParms);
        settings.setId(UUID.randomUUID());
        
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        
        DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(settings));
        TransformIterator iter = new DatawaveTransformIterator(logic.iterator(), transformer);
        List<Object> eventList = new ArrayList<>();
        while (iter.hasNext()) {
            eventList.add(iter.next());
        }
        
        BaseQueryResponse response = transformer.createResponse(eventList);
        
        // un-comment to look at the json output
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        mapper.writeValue(temporaryFolder.newFile(), response);
        
        Assert.assertTrue(response instanceof DefaultEventQueryResponse);
        DefaultEventQueryResponse eventQueryResponse = (DefaultEventQueryResponse) response;
        
        Assert.assertEquals("Got the wrong number of events", expected.size(), (long) eventQueryResponse.getReturnedEvents());
        
        for (EventBase event : eventQueryResponse.getEvents()) {
            
            String firstKey = "";
            String secondKey = "";
            Integer value = null;
            for (Object field : event.getFields()) {
                FieldBase fieldBase = (FieldBase) field;
                switch (fieldBase.getName()) {
                    case "COUNT":
                        value = Integer.valueOf(fieldBase.getValueString());
                        break;
                    case "GENDER":
                    case "GEN":
                    case "BIRTHDAY":
                        firstKey = fieldBase.getValueString();
                        break;
                    case "AGE":
                    case "AG":
                    case "RECORD":
                        secondKey = fieldBase.getValueString();
                        break;
                }
            }
            
            log.debug("mapping is " + firstKey + "-" + secondKey + " count:" + value);
            String key;
            if (!firstKey.isEmpty() && !secondKey.isEmpty()) {
                key = firstKey + "-" + secondKey;
            } else if (!firstKey.isEmpty()) {
                key = firstKey;
            } else {
                key = secondKey;
            }
            Assert.assertEquals(expected.get(key), value);
        }
        return response;
    }
    
    @Test
    public void testGrouping() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("FEMALE-18", 2)
                .put("MALE-30", 1)
                .put("MALE-34", 1)
                .put("MALE-16", 1)
                .put("MALE-40", 2)
                .put("MALE-20", 2)
                .put("MALE-24", 1)
                .put("MALE-22", 2)
                .build();
        // @formatter:on
        
        extraParameters.put("group.fields", "AGE,$GENDER");
        extraParameters.put("group.fields.batch.size", "6");
        
        List<List<EventBase>> responseEvents = new ArrayList<>();
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                responseEvents.add(((DefaultEventQueryResponse) runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters,
                                teardown, interrupt)).getEvents());
            }
        }
        List<String> digested = digest(responseEvents);
        log.debug("reponses:" + digested);
        Set<String> responseSet = Sets.newHashSet(digested);
        // if the grouped results from every type of rebuild are the same, there should be only 1 entry in the responseSet
        Assert.assertEquals(responseSet.size(), 1);
    }
    
    // grab the relevant stuff from the events and do some formatting
    private List<String> digest(List<List<EventBase>> in) {
        List<String> stringList = new ArrayList<>();
        for (List<EventBase> list : in) {
            StringBuilder builder = new StringBuilder();
            for (EventBase eb : list) {
                for (Object field : eb.getFields()) {
                    FieldBase fieldBase = (FieldBase) field;
                    builder.append(fieldBase.getName());
                    builder.append(':');
                    builder.append(fieldBase.getTypedValue().getValue());
                    builder.append(',');
                }
            }
            stringList.add(builder.toString() + '\n');
        }
        return stringList;
    }
    
    @Test
    public void testGrouping2() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("18", 2)
                .put("30", 1)
                .put("34", 1)
                .put("16", 1)
                .put("40", 2)
                .put("20", 2)
                .put("24", 1)
                .put("22", 2)
                .build();
        // @formatter:on
        extraParameters.put("group.fields", "AGE");
        extraParameters.put("group.fields.batch.size", "6");
        
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters, teardown, interrupt);
            }
        }
    }
    
    @Test
    public void testGrouping3() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        Map<String,Integer> expectedMap = ImmutableMap.of("MALE", 10, "FEMALE", 2);
        
        extraParameters.put("group.fields", "GENDER");
        extraParameters.put("group.fields.batch.size", "0");
        
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters, teardown, interrupt);
            }
        }
    }
    
    @Test
    public void testGrouping4() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        Map<String,Integer> expectedMap = ImmutableMap.of("MALE", 10, "FEMALE", 2);
        
        extraParameters.put("group.fields", "GENDER");
        extraParameters.put("group.fields.batch.size", "6");
        
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters, teardown, interrupt);
            }
        }
    }
    
    @Test
    public void testGroupingEntriesWithNoContext() throws Exception {
        // Testing multivalued entries with no grouping context
        Map<String,String> extraParameters = new HashMap<>();
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        Map<String,Integer> expectedMap = ImmutableMap.of("1", 3, "2", 3, "3", 1);
        
        extraParameters.put("group.fields", "RECORD");
        
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters, teardown, interrupt);
            }
        }
    }
    
    @Test
    public void testGroupingMixedEntriesWithAndWithNoContext() throws Exception {
        // Testing multivalued entries with no grouping context in combination with a grouping context entries
        Map<String,String> extraParameters = new HashMap<>();
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("FEMALE-2", 1)
                .put("MALE-1", 3)
                .put("MALE-2", 2)
                .put("MALE-3", 1)
                .build();
        // @formatter:on
        
        extraParameters.put("group.fields", "GENDER,RECORD");
        // extraParameters.put("group.fields.batch.size", "12");
        
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters, teardown, interrupt);
            }
        }
    }
    
    @Test
    public void testGroupingUsingFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("group.fields.batch.size", "6");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*' && f:groupby('$AGE','GENDER')";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("FEMALE-18", 2)
                .put("MALE-30", 1)
                .put("MALE-34", 1)
                .put("MALE-16", 1)
                .put("MALE-40", 2)
                .put("MALE-20", 2)
                .put("MALE-24", 1)
                .put("MALE-22", 2)
                .build();
        // @formatter:on
        
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters, teardown, interrupt);
            }
        }
    }
    
    @Test
    public void testGroupingUsingLuceneFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("group.fields.batch.size", "6");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "(UUID:C* or UUID:S* ) and #GROUPBY('AGE','$GENDER')";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("FEMALE-18", 2)
                .put("MALE-30", 1)
                .put("MALE-34", 1)
                .put("MALE-16", 1)
                .put("MALE-40", 2)
                .put("MALE-20", 2)
                .put("MALE-24", 1)
                .put("MALE-22", 2)
                .build();
        // @formatter:on
        logic.setParser(new LuceneToJexlQueryParser());
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters, teardown, interrupt);
            }
        }
        logic.setParser(new JexlControlledQueryParser());
    }
    
    @Test
    public void testGroupingUsingLuceneFunctionWithDuplicateValues() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("group.fields.batch.size", "6");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "(UUID:CORLEONE) and #GROUPBY('AGE','BIRTHDAY')";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("4-18", 1)
                .put("5-40", 1)
                .put("3-20", 1)
                .put("1-24", 1)
                .put("2-22", 1)
                .put("22-22", 1)
                .build();
        // @formatter:on
        logic.setParser(new LuceneToJexlQueryParser());
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters, teardown, interrupt);
            }
        }
        logic.setParser(new JexlControlledQueryParser());
    }
    
    @Test
    public void testCountingMap() {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        GroupingTransform.GroupCountingHashMap map = new GroupingTransform.GroupCountingHashMap(markingFunctions);
        GroupingTypeAttribute attr1 = new GroupingTypeAttribute(new LcType("FOO"), new Key("FOO"), true);
        attr1.setColumnVisibility(new ColumnVisibility("A"));
        map.add(Collections.singleton(attr1));
        
        GroupingTypeAttribute attr2 = new GroupingTypeAttribute(new LcType("FOO"), new Key("FOO"), true);
        attr2.setColumnVisibility(new ColumnVisibility("B"));
        map.add(Collections.singleton(attr2));
        GroupingTypeAttribute attr3 = new GroupingTypeAttribute(new LcType("BAR"), new Key("BAR"), true);
        attr3.setColumnVisibility(new ColumnVisibility("C"));
        map.add(Collections.singleton(attr3));
        
        log.debug("map is: " + map);
        
        for (Map.Entry<Collection<GroupingTypeAttribute<?>>,Integer> entry : map.entrySet()) {
            Attribute<?> attr = entry.getKey().iterator().next(); // the first and only one
            int count = entry.getValue();
            if (attr.getData().toString().equals("FOO")) {
                Assert.assertEquals(2, count);
                Assert.assertEquals(new ColumnVisibility("A&B"), attr.getColumnVisibility());
            } else if (attr.getData().toString().equals("BAR")) {
                Assert.assertEquals(1, count);
                Assert.assertEquals(new ColumnVisibility("C"), attr.getColumnVisibility());
            }
        }
    }
    
    @Test
    public void testCountingMapAgain() {
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        GroupingTransform.GroupCountingHashMap map = new GroupingTransform.GroupCountingHashMap(markingFunctions);
        
        GroupingTypeAttribute<?> attr1a = new GroupingTypeAttribute(new LcType("FOO"), new Key("NAME"), true);
        attr1a.setColumnVisibility(new ColumnVisibility("A"));
        GroupingTypeAttribute<?> attr1b = new GroupingTypeAttribute(new NumberType("5"), new Key("AGE"), true);
        attr1b.setColumnVisibility(new ColumnVisibility("C"));
        Set<GroupingTypeAttribute<?>> seta = Sets.newHashSet(attr1a, attr1b);
        map.add(seta);
        
        GroupingTypeAttribute<?> attr2a = new GroupingTypeAttribute(new LcType("FOO"), new Key("NAME"), true);
        attr2a.setColumnVisibility(new ColumnVisibility("B"));
        GroupingTypeAttribute<?> attr2b = new GroupingTypeAttribute(new NumberType("5"), new Key("AGE"), true);
        attr2b.setColumnVisibility(new ColumnVisibility("D"));
        Set<GroupingTypeAttribute<?>> setb = Sets.newHashSet(attr2a, attr2b);
        map.add(setb);
        
        // even though the ColumnVisibilities are different, the 2 collections seta and setb are 'equal' and generate the same hashCode
        Assert.assertEquals(seta.hashCode(), setb.hashCode());
        Assert.assertEquals(seta, setb);
        
        GroupingTypeAttribute attr3a = new GroupingTypeAttribute(new LcType("BAR"), new Key("NAME"), true);
        attr3a.setColumnVisibility(new ColumnVisibility("C"));
        GroupingTypeAttribute attr3b = new GroupingTypeAttribute(new NumberType("6"), new Key("AGE"), true);
        attr3b.setColumnVisibility(new ColumnVisibility("D"));
        map.add(Sets.newHashSet(attr3a, attr3b));
        
        log.debug("map is: " + map);
        
        for (Map.Entry<Collection<GroupingTypeAttribute<?>>,Integer> entry : map.entrySet()) {
            for (Attribute<?> attr : entry.getKey()) {
                int count = entry.getValue();
                if (attr.getData().toString().equals("FOO")) {
                    Assert.assertEquals(2, count);
                    // the ColumnVisibility for the key was changed to the merged value of the 2 items that were added to the map
                    Assert.assertEquals(new ColumnVisibility("A&B"), attr.getColumnVisibility());
                } else if (attr.getData().toString().equals("5")) {
                    Assert.assertEquals(2, count);
                    // the ColumnVisibility for the key was changed to the merged value of the 2 items that were added to the map
                    Assert.assertEquals(new ColumnVisibility("C&D"), attr.getColumnVisibility());
                } else if (attr.getData().toString().equals("BAR")) {
                    Assert.assertEquals(1, count);
                    Assert.assertEquals(new ColumnVisibility("C"), attr.getColumnVisibility());
                } else if (attr.getData().toString().equals("6")) {
                    Assert.assertEquals(1, count);
                    Assert.assertEquals(new ColumnVisibility("D"), attr.getColumnVisibility());
                }
            }
        }
    }
    
    private static RemoteEdgeDictionary mockRemoteEdgeDictionary = EasyMock.createMock(RemoteEdgeDictionary.class);
    
    public static class Producer {
        @Produces
        public static RemoteEdgeDictionary produceRemoteEdgeDictionary() {
            return mockRemoteEdgeDictionary;
        }
    }
    
}
