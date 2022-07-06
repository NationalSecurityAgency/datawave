package datawave.query;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.util.TableName;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 * This test confirms that hit terms are found in the correct documents, and only in the correct documents. The test data has fields that will hit in different
 * grouping context levels, and assures that the hits contain the fields with the correct grouping context. It also confirms that an 'or' query that hits
 * different fields in the returned documents will have the correct hit terms.
 *
 * The same tests are made against document ranges and shard ranges
 * 
 */
public abstract class UseOccurrenceToCountInJexlContextTest {
    
    private static final Logger log = Logger.getLogger(UseOccurrenceToCountInJexlContextTest.class);
    
    public static class ShardRange extends UseOccurrenceToCountInJexlContextTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D,T,U,V,W,X,Y,Z");
            QueryTestTableHelper qtth = new QueryTestTableHelper(UseOccurrenceToCountInJexlContextTest.ShardRange.class.toString(), log);
            connector = qtth.connector;
            
            MoreTestData.writeItAll(connector, MoreTestData.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("A", "B", "C", "D");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                        Multimap<String,String> expectedHitTerms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, expectedHitTerms, connector);
        }
    }
    
    public static class DocumentRange extends UseOccurrenceToCountInJexlContextTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D,T,U,V,W,X,Y,Z");
            QueryTestTableHelper qtth = new QueryTestTableHelper(UseOccurrenceToCountInJexlContextTest.DocumentRange.class.toString(), log);
            connector = qtth.connector;
            
            MoreTestData.writeItAll(connector, MoreTestData.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("A", "B", "C", "D");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                        Multimap<String,String> expectedHitTerms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, expectedHitTerms, connector);
        }
    }
    
    protected Authorizations auths = new Authorizations("A", "B", "C", "D");
    
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    
    protected ShardQueryLogic logic = null;
    
    protected KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @SuppressWarnings("unchecked")
    Multimap<String,String>[] expectedHitTerms = new Multimap[] {
    
    new ImmutableListMultimap.Builder<String,String>().put("Second", "NAME.2:Haiqu").put("Second", "BAR.0:BAR").build(),
    
    new ImmutableListMultimap.Builder<String,String>().put("Third", "NAME.0:NAME9").put("Third", "NAME.1:NAME8").put("Third", "NAME.2:Haiqu").build(),
    
    new ImmutableListMultimap.Builder<String,String>().put("Second", "UUID.0:Second").build(),
    
    };
    
    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }
    
    @Before
    public void setup() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        logic = new ShardQueryLogic();
        logic.setMetadataTableName(QueryTestTableHelper.METADATA_TABLE_NAME);
        logic.setTableName(TableName.SHARD);
        logic.setIndexTableName(TableName.SHARD_INDEX);
        logic.setReverseIndexTableName(TableName.SHARD_RINDEX);
        logic.setMaxResults(5000);
        logic.setMaxWork(25000);
        logic.setModelTableName(QueryTestTableHelper.METADATA_TABLE_NAME);
        logic.setQueryPlanner(new DefaultQueryPlanner());
        logic.setIncludeGroupingContext(true);
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        logic.setMaxEvaluationPipelines(1);
        deserializer = new KryoDocumentDeserializer();
    }
    
    protected abstract void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    Multimap<String,String> expectedHitTerms) throws Exception;
    
    protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    Multimap<String,String> expectedHitTerms, Connector connector) throws Exception {
        log.debug("runTestQuery");
        log.trace("Creating QueryImpl");
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(querystr);
        settings.setParameters(extraParms);
        settings.setId(UUID.randomUUID());
        settings.setParameters(Collections.singletonMap("hit.list", "true"));
        
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        
        HashSet<String> expectedSet = new HashSet<>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<>();
        Set<Document> docs = new HashSet<>();
        for (Entry<Key,Value> entry : logic) {
            
            Document d = deserializer.apply(entry).getValue();
            
            log.debug(entry.getKey() + " => " + d);
            
            Attribute<?> attr = d.get("UUID.0");
            
            Assert.assertNotNull("Result Document did not contain a 'UUID'", attr);
            Assert.assertTrue("Expected result to be an instance of DatwawaveTypeAttribute, was: " + attr.getClass().getName(), attr instanceof TypeAttribute
                            || attr instanceof PreNormalizedAttribute);
            
            TypeAttribute<?> uuidAttr = (TypeAttribute<?>) attr;
            
            String uuid = uuidAttr.getType().getDelegate().toString();
            Assert.assertTrue("Received unexpected UUID: " + uuid, expected.contains(uuid));
            
            Attribute<?> hitTermAttribute = d.get(JexlEvaluation.HIT_TERM_FIELD);
            if (hitTermAttribute instanceof Attributes) {
                
                Attributes hitTerms = (Attributes) hitTermAttribute;
                for (Attribute<?> hitTerm : hitTerms.getAttributes()) {
                    log.debug("hitTerm:" + hitTerm);
                    String hitString = hitTerm.getData().toString();
                    log.debug("as string:" + hitString);
                    log.debug("expectedHitTerms:" + expectedHitTerms);
                    // make sure this hitString is in the map, and remove it
                    boolean result = expectedHitTerms.get(uuid).remove(hitString);
                    if (result == false) {
                        log.debug("failed to find hitString:" + hitString + " for uuid:" + uuid + " in expectedHitTerms:" + expectedHitTerms
                                        + " from hitTerms:" + hitTerms);
                        Assert.fail("failed to find hitString:" + hitString + " for uuid:" + uuid + " in expectedHitTerms:" + expectedHitTerms
                                        + " from hitTerms:" + hitTerms);
                    } else {
                        log.debug("removed hitString:" + hitString + " for uuid:" + uuid + " in expectedHitTerms:" + expectedHitTerms + " from hitTerms:"
                                        + hitTerms);
                    }
                }
            } else if (hitTermAttribute instanceof Attribute) {
                log.debug("hitTerm:" + (Attribute<?>) hitTermAttribute);
                String hitString = ((Attribute<?>) hitTermAttribute).getData().toString();
                log.debug("as string:" + hitString);
                log.debug("expectedHitTerms:" + expectedHitTerms);
                boolean result = expectedHitTerms.get(uuid).remove(hitString);
                if (result == false) {
                    log.debug("failed to find hitString:" + hitString + " for uuid:" + uuid + " in expectedHitTerms:" + expectedHitTerms);
                    Assert.fail("failed to find hitString:" + hitString + " for uuid:" + uuid + " in expectedHitTerms:" + expectedHitTerms);
                } else {
                    log.debug("removed hitString:" + hitString + " for uuid:" + uuid + " in expectedHitTerms:" + expectedHitTerms + " from hitTerm:"
                                    + (Attribute<?>) hitTermAttribute);
                }
            }
            
            resultSet.add(uuid);
            docs.add(d);
        }
        
        if (expected.size() > resultSet.size()) {
            expectedSet.addAll(expected);
            expectedSet.removeAll(resultSet);
            
            for (String s : expectedSet) {
                log.warn("Missing: " + s);
            }
        }
        
        if (!expected.containsAll(resultSet)) {
            log.error("Expected results " + expected + " differ form actual results " + resultSet);
        }
        Assert.assertTrue("Expected results " + expected + " differ form actual results " + resultSet, expected.containsAll(resultSet));
        Assert.assertEquals("Unexpected number of records", expected.size(), resultSet.size());
        
        // the map is empty if there were no unexpected hit terms in it
        log.debug("expectedHitTerms:" + expectedHitTerms);
        Assert.assertTrue(expectedHitTerms.isEmpty());
    }
    
    @Test
    public void runTest() throws Exception {
        
        Map<String,String> extraParameters = new HashMap<>();
        
        // @formatter:off
        String[] queryStrings = {
                "NAME == 'haiqu' && BAR == 'bar' && filter:occurrence(NAME, '==', 3)",
                "NAME == 'haiqu' && filter:occurrence(NAME, '==', filter:getAllMatches(NAME, 'NAME9').size() + filter:getAllMatches(NAME, 'NAME8').size() + filter:getAllMatches(NAME, 'Haiqu').size())",
                "UUID == 'Second'"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                // just the expected uuids. I should always get both documents, the real test is in the hit terms
                Arrays.asList("Second"),
                Arrays.asList("Third"),
                Arrays.asList("Second")
        };
        // @formatter:on
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters,
                            ArrayListMultimap.create(expectedHitTerms[i]));
        }
    }
    
    private static class MoreTestData {
        
        enum WhatKindaRange {
            SHARD, DOCUMENT;
        }
        
        private static final Type<?> lcNoDiacriticsType = new LcNoDiacriticsType();
        
        protected static final String datatype = "test";
        protected static final String date = "20130101";
        protected static final String shard = date + "_0";
        protected static final ColumnVisibility columnVisibility = new ColumnVisibility("C");
        protected static final Value emptyValue = new Value(new byte[0]);
        protected static final long timeStamp = 1356998400000l;
        
        /**
         */
        public static void writeItAll(Connector con, WhatKindaRange range) throws Exception {
            BatchWriter bw = null;
            BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
            Mutation mutation = null;
            
            String firstUID = UID.builder().newId("First".getBytes(), (Date) null).toString();
            String secondUID = UID.builder().newId("Second".getBytes(), (Date) null).toString();
            String thirdUID = UID.builder().newId("Third".getBytes(), (Date) null).toString();
            
            try {
                // write the shard table :
                bw = con.createBatchWriter(TableName.SHARD, bwConfig);
                mutation = new Mutation(shard);
                // NAME.0 gets NAME0 and NAME.1 gets NAME1
                mutation.put(datatype + "\u0000" + firstUID, "NAME.0" + "\u0000" + "NAME0", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + firstUID, "NAME.1" + "\u0000" + "NAME1", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + firstUID, "NAME.2" + "\u0000" + "Haiqu", columnVisibility, timeStamp, emptyValue);
                // FOO is only in First
                mutation.put(datatype + "\u0000" + firstUID, "FOO.0" + "\u0000" + "FOO", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + firstUID, "UUID.0" + "\u0000" + "First", columnVisibility, timeStamp, emptyValue);
                
                // this event has NAME1 and NAME0 in the opposite fields
                mutation.put(datatype + "\u0000" + secondUID, "NAME.0" + "\u0000" + "NAME1", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + secondUID, "NAME.1" + "\u0000" + "NAME0", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + secondUID, "NAME.2" + "\u0000" + "Haiqu", columnVisibility, timeStamp, emptyValue);
                // BAR is only in Second
                mutation.put(datatype + "\u0000" + secondUID, "BAR.0" + "\u0000" + "BAR", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + secondUID, "UUID.0" + "\u0000" + "Second", columnVisibility, timeStamp, emptyValue);
                
                mutation.put(datatype + "\u0000" + thirdUID, "NAME.0" + "\u0000" + "NAME9", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + thirdUID, "NAME.1" + "\u0000" + "NAME8", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + thirdUID, "NAME.2" + "\u0000" + "Haiqu", columnVisibility, timeStamp, emptyValue);
                // BAZ is only in Third
                mutation.put(datatype + "\u0000" + thirdUID, "BAZ.0" + "\u0000" + "BAZ", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + thirdUID, "UUID.0" + "\u0000" + "Third", columnVisibility, timeStamp, emptyValue);
                
                bw.addMutation(mutation);
                
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }
            
            try {
                // write shard index table:
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("First"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(firstUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Second"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(secondUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Third"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(thirdUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Haiqu"));
                mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(firstUID, secondUID, thirdUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("NAME0"));
                mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(firstUID, secondUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("NAME1"));
                mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(firstUID, secondUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("NAME8"));
                mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(thirdUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("NAME9"));
                mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(thirdUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("BAR"));
                mutation.put("BAR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(secondUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("BAZ"));
                mutation.put("BAR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(thirdUID));
                bw.addMutation(mutation);
                
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }
            
            try {
                
                // write the reverse index table:
                bw = con.createBatchWriter(TableName.SHARD_RINDEX, bwConfig);
                mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("First")).reverse());
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(firstUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_RINDEX, bwConfig);
                mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("Second")).reverse());
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(secondUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(TableName.SHARD_RINDEX, bwConfig);
                mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("Third")).reverse());
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(thirdUID));
                bw.addMutation(mutation);
                
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }
            
            try {
                
                // write the field index table:
                bw = con.createBatchWriter(TableName.SHARD, bwConfig);
                mutation = new Mutation(shard);
                
                mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("First") + "\u0000" + datatype + "\u0000" + firstUID, columnVisibility,
                                timeStamp, emptyValue);
                
                mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("Second") + "\u0000" + datatype + "\u0000" + secondUID, columnVisibility,
                                timeStamp, emptyValue);
                
                mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("Third") + "\u0000" + datatype + "\u0000" + thirdUID, columnVisibility,
                                timeStamp, emptyValue);
                
                mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("Haiqu") + "\u0000" + datatype + "\u0000" + firstUID, columnVisibility,
                                timeStamp, emptyValue);
                mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("Haiqu") + "\u0000" + datatype + "\u0000" + secondUID, columnVisibility,
                                timeStamp, emptyValue);
                mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("Haiqu") + "\u0000" + datatype + "\u0000" + thirdUID, columnVisibility,
                                timeStamp, emptyValue);
                
                mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("NAME0") + "\u0000" + datatype + "\u0000" + firstUID, columnVisibility,
                                timeStamp, emptyValue);
                mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("NAME0") + "\u0000" + datatype + "\u0000" + secondUID, columnVisibility,
                                timeStamp, emptyValue);
                
                mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("NAME1") + "\u0000" + datatype + "\u0000" + firstUID, columnVisibility,
                                timeStamp, emptyValue);
                mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("NAME1") + "\u0000" + datatype + "\u0000" + secondUID, columnVisibility,
                                timeStamp, emptyValue);
                
                mutation.put("fi\u0000" + "BAR", lcNoDiacriticsType.normalize("BAR") + "\u0000" + datatype + "\u0000" + secondUID, columnVisibility, timeStamp,
                                emptyValue);
                
                mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("NAME8") + "\u0000" + datatype + "\u0000" + thirdUID, columnVisibility,
                                timeStamp, emptyValue);
                mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("NAME9") + "\u0000" + datatype + "\u0000" + thirdUID, columnVisibility,
                                timeStamp, emptyValue);
                mutation.put("fi\u0000" + "BAR", lcNoDiacriticsType.normalize("BAZ") + "\u0000" + datatype + "\u0000" + thirdUID, columnVisibility, timeStamp,
                                emptyValue);
                
                bw.addMutation(mutation);
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }
            
            try {
                // write metadata table:
                bw = con.createBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME, bwConfig);
                
                mutation = new Mutation("NAME");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                bw.addMutation(mutation);
                
                mutation = new Mutation("FOO");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                bw.addMutation(mutation);
                
                mutation = new Mutation("BAR");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                bw.addMutation(mutation);
                
                mutation = new Mutation("BAZ");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                bw.addMutation(mutation);
                
                mutation = new Mutation("UUID");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
                bw.addMutation(mutation);
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }
        }
    }
    
    /**
     * allows a document specific range
     * 
     * @param in
     * @return
     */
    private static Value getValueForBuilderFor(String... in) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        for (String s : in) {
            builder.addUID(s);
        }
        builder.setCOUNT(in.length);
        builder.setIGNORE(false);
        return new Value(builder.build().toByteArray());
    }
    
    /**
     * forces a shard range
     * 
     * @return
     */
    private static Value getValueForNuthinAndYourHitsForFree() {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setCOUNT(50); // better not be zero!!!!
        builder.setIGNORE(true); // better be true!!!
        return new Value(builder.build().toByteArray());
    }
}
