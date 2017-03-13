package nsa.datawave.query.rewrite;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import nsa.datawave.data.ColumnFamilyConstants;
import nsa.datawave.data.hash.UID;
import nsa.datawave.data.type.LcNoDiacriticsType;
import nsa.datawave.data.type.Type;
import nsa.datawave.helpers.PrintUtility;
import nsa.datawave.ingest.data.TypeRegistry;
import nsa.datawave.ingest.protobuf.Uid;
import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.query.rewrite.attributes.Attribute;
import nsa.datawave.query.rewrite.attributes.Attributes;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.attributes.PreNormalizedAttribute;
import nsa.datawave.query.rewrite.attributes.TypeAttribute;
import nsa.datawave.query.rewrite.function.deserializer.KryoDocumentDeserializer;
import nsa.datawave.query.rewrite.planner.DefaultQueryPlanner;
import nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import nsa.datawave.query.util.DateIndexHelperFactory;
import nsa.datawave.query.util.MetadataHelperFactory;
import nsa.datawave.query.util.TypeMetadata;
import nsa.datawave.query.util.TypeMetadataHelper;
import nsa.datawave.query.util.TypeMetadataWriter;
import nsa.datawave.security.util.ScannerHelper;
import nsa.datawave.webservice.query.QueryImpl;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
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

import static nsa.datawave.query.rewrite.QueryTestTableHelper.MODEL_TABLE_NAME;
import static nsa.datawave.query.rewrite.QueryTestTableHelper.SHARD_INDEX_TABLE_NAME;
import static nsa.datawave.query.rewrite.QueryTestTableHelper.SHARD_RINDEX_TABLE_NAME;
import static nsa.datawave.query.rewrite.QueryTestTableHelper.SHARD_TABLE_NAME;

/**
 *
 * This test confirms that hit terms are found in the correct documents, and only in the correct documents. The test data has fields that will hit in different
 * grouping context levels, and assures that the hits contain the fields with the correct grouping context. It also confirms that an 'or' query that hits
 * different fields in the returned documents will have the correct hit terms.
 *
 * The same tests are made against document ranges and shard ranges
 * 
 */
public class IfThisTestFailsThenHitTermsAreBroken {
    
    static enum WhatKindaRange {
        SHARD, DOCUMENT;
    }
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    private static List<TemporaryFolder> staticTempFolders = Lists.newArrayList();
    
    private static final Logger log = Logger.getLogger(IfThisTestFailsThenHitTermsAreBroken.class);
    
    protected static Connector connector = null;
    
    protected Authorizations auths = new Authorizations("A");
    
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    
    protected RefactoredShardQueryLogic logic = null;
    
    protected KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @SuppressWarnings("unchecked")
    Multimap<String,String>[] expectedHitTerms = new Multimap[] {
            
            new ImmutableListMultimap.Builder<String,String>().put("First", "UUID.0:First").put("Second", "UUID.0:Second").build(),
            
            new ImmutableListMultimap.Builder<String,String>().put("First", "FOO.0:FOO").put("First", "UUID.0:First").put("Second", "BAR.0:BAR")
                            .put("Second", "UUID.0:Second").build(),
            
            new ImmutableListMultimap.Builder<String,String>().put("First", "NAME.0:NAME0").put("First", "UUID.0:First").put("Second", "NAME.1:NAME0")
                            .put("Second", "UUID.0:Second").build(),
            
            new ImmutableListMultimap.Builder<String,String>().put("First", "UUID.0:First").put("First", "NAME.2:Haiqu").put("First", "FOO.0:FOO").build(),
            
            new ImmutableListMultimap.Builder<String,String>().put("Second", "UUID.0:Second").put("Second", "NAME.2:Haiqu").put("Second", "BAR.0:BAR").build(),
            
            new ImmutableListMultimap.Builder<String,String>().put("Second", "UUID.0:Second").put("Second", "NAME.2:Haiqu").put("Second", "BAR.0:BAR").build(),
            
            new ImmutableListMultimap.Builder<String,String>().put("Second", "UUID.0:Second").put("Second", "NAME.2:Haiqu").put("Second", "BAR.0:BAR").build(),
            
            new ImmutableListMultimap.Builder<String,String>().put("Second", "UUID.0:Second").put("Second", "BAR.0:BAR").build(),
            
            new ImmutableListMultimap.Builder<String,String>().put("Second", "NAME.2:Haiqu").put("Second", "BAR.0:BAR").build(),
            
            new ImmutableListMultimap.Builder<String,String>().put("First", "NAME.0:NAME0").put("First", "UUID.0:First").put("First", "NAME.1:NAME1")
                            .put("First", "NAME.2:Haiqu").build(),};
    
    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
        for (TemporaryFolder t : staticTempFolders) {
            if (t.getRoot().exists()) {
                try {
                    FileUtils.deleteDirectory(t.getRoot());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            log.debug("after class " + t.getRoot() + " exists? " + t.getRoot().exists());
        }
    }
    
    @After
    public void after() {
        TypeRegistry.reset();
        System.clearProperty("type.metadata.dir");
        staticTempFolders.add(tempFolder);
        for (TemporaryFolder t : staticTempFolders) {
            log.debug("after test " + t.getRoot() + " exists? " + t.getRoot().exists());
        }
    }
    
    @Before
    public void setup() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        String val = tempFolder.getRoot().getAbsolutePath();
        System.setProperty("type.metadata.dir", val);
        log.info("using tempFolder " + tempFolder.getRoot());
        
        logic = new RefactoredShardQueryLogic();
        logic.setMetadataTableName(MODEL_TABLE_NAME);
        logic.setTableName(SHARD_TABLE_NAME);
        logic.setIndexTableName(SHARD_INDEX_TABLE_NAME);
        logic.setReverseIndexTableName(SHARD_RINDEX_TABLE_NAME);
        logic.setMaxResults(5000);
        logic.setMaxRowsToScan(25000);
        logic.setModelTableName(MODEL_TABLE_NAME);
        logic.setQueryPlanner(new DefaultQueryPlanner());
        logic.setIncludeGroupingContext(true);
        logic.setMarkingFunctions(new MarkingFunctions.NoOp());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        logic.setMaxEvaluationPipelines(1);
        deserializer = new KryoDocumentDeserializer();
    }
    
    public void debugQuery(String tableName) throws Exception {
        Scanner s = ScannerHelper.createScanner(connector, tableName, authSet);
        Range r = new Range();
        s.setRange(r);
        for (Entry<Key,Value> entry : s) {
            if (log.isDebugEnabled()) {
                log.debug(entry.getKey().toString() + " " + entry.getValue().toString());
            }
        }
    }
    
    protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    Multimap<String,String> expectedHitTerms) throws ParseException, Exception {
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
        settings.setParameters(extraParms);
        
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        
        TypeMetadataWriter typeMetadataWriter = TypeMetadataWriter.Factory.createTypeMetadataWriter();
        TypeMetadataHelper typeMetadataHelper = new TypeMetadataHelper();
        typeMetadataHelper.initialize(connector, MODEL_TABLE_NAME, authSet);
        Map<Set<String>,TypeMetadata> typeMetadataMap = typeMetadataHelper.getTypeMetadataMap(authSet);
        typeMetadataWriter.writeTypeMetadataMap(typeMetadataMap, MODEL_TABLE_NAME);
        
        HashSet<String> expectedSet = new HashSet<String>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<String>();
        Set<Document> docs = new HashSet<Document>();
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
            
            Attribute<?> hitTermAttribute = d.get("HIT_TERM");
            if (hitTermAttribute instanceof Attributes) {
                
                Attributes hitTerms = (Attributes) hitTermAttribute;
                for (Attribute<?> hitTerm : hitTerms.getAttributes()) {
                    log.debug("hitTerm:" + hitTerm);
                    String hitString = hitTerm.getData().toString();
                    log.debug("as string:" + hitString);
                    log.debug("expectedHitTerms:" + expectedHitTerms);
                    Assert.assertNotEquals(hitTerm.getTimestamp(), Long.MAX_VALUE);
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
                Attribute<?> hitTerm = (Attribute<?>) hitTermAttribute;
                log.debug("hitTerm:" + hitTerm);
                String hitString = hitTerm.getData().toString();
                log.debug("as string:" + hitString);
                log.debug("expectedHitTerms:" + expectedHitTerms);
                boolean result = expectedHitTerms.get(uuid).remove(hitString);
                if (result == false) {
                    log.debug("failed to find hitString:" + hitString + " for uuid:" + uuid + " in expectedHitTerms:" + expectedHitTerms);
                    Assert.fail("failed to find hitString:" + hitString + " for uuid:" + uuid + " in expectedHitTerms:" + expectedHitTerms);
                } else {
                    log.debug("removed hitString:" + hitString + " for uuid:" + uuid + " in expectedHitTerms:" + expectedHitTerms + " from hitTerm:" + hitTerm);
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
    public void testWithShardRange() throws Exception {
        
        QueryTestTableHelper qtth = new QueryTestTableHelper(IfThisTestFailsThenHitTermsAreBroken.class.toString(), log);
        connector = qtth.connector;
        
        MoreTestData.writeItAll(connector, WhatKindaRange.SHARD);
        if (log.isDebugEnabled()) {
            log.debug("testWithShardRange");
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        doIt(WhatKindaRange.SHARD);
    }
    
    @Test
    public void testWithDocumentRange() throws Exception {
        
        QueryTestTableHelper qtth = new QueryTestTableHelper(IfThisTestFailsThenHitTermsAreBroken.class.toString(), log);
        connector = qtth.connector;
        
        MoreTestData.writeItAll(connector, WhatKindaRange.DOCUMENT);
        if (log.isDebugEnabled()) {
            log.debug("testWithDocumentRange");
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        doIt(WhatKindaRange.DOCUMENT);
    }
    
    private void doIt(WhatKindaRange wkrange) throws Exception {
        
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("type.metadata.in.hdfs", "true");
        extraParameters.put("hit.list", "true");
        
        String[] queryStrings = {
                // sanity check. I got the 2 documents
                "UUID == 'First' || UUID == 'Second'",
                // look for FOO or BAR, expecting the hit_terms to be in the right places
                "( UUID == 'First' || UUID == 'Second' ) && ( FOO == 'FOO' || BAR == 'BAR' )",
                // should find NAME0 in different grouping contexts, but the hit terms should be correct
                "( UUID == 'First' || UUID == 'Second' ) &&  NAME == 'NAME0'",
                // this may get initial hits in Second, but will return only First. Makes sure that hits from Second are not included in First
                "( UUID == 'First' || UUID == 'Second' ) &&  NAME == 'Haiqu' && FOO == 'FOO'",
                // this may get initial hits in First, but will return only Second. Makes sure that hits from First are not included in Second
                "( UUID == 'First' || UUID == 'Second' ) &&  NAME == 'Haiqu' && BAR == 'BAR'",
                // try to pull in hits from Third, should still hit only Second
                "( UUID == 'First' || UUID == 'Second' || UUID == 'Third') &&  NAME == 'Haiqu' && BAR == 'BAR'",
                
                "( UUID == 'First' || UUID == 'Second' || UUID == 'Third') &&  filter:includeRegex(NAME,'Haiqu') && filter:includeRegex(BAR,'BAR')",
                
                "UUID == 'Second' && BAR == 'BAR'",
                
                "NAME == 'Haiqu' && BAR == 'BAR' && filter:occurrence(NAME, '==', 3)",
                
                "UUID == 'First' && filter:isNotNull(NAME)"
        
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                // just the expected uuids. I should always get both documents, the real test is in the hit terms
                Arrays.asList("First", "Second"), Arrays.asList("First", "Second"), Arrays.asList("First", "Second"), Arrays.asList("First"),
                Arrays.asList("Second"), Arrays.asList("Second"), Arrays.asList("Second"), Arrays.asList("Second"), Arrays.asList("Second"),
                Arrays.asList("First")};
        
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters,
                            ArrayListMultimap.create(expectedHitTerms[i]));
        }
    }
    
    private static class MoreTestData {
        
        private static final Type<?> lcNoDiacriticsType = new LcNoDiacriticsType();
        
        protected static final String datatype = "test";
        protected static final String date = "20130101";
        protected static final String shard = date + "_0";
        protected static final ColumnVisibility columnVisibility = new ColumnVisibility("A");
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
                bw = con.createBatchWriter(SHARD_TABLE_NAME, bwConfig);
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
                bw = con.createBatchWriter(SHARD_INDEX_TABLE_NAME, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("First"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(firstUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(SHARD_INDEX_TABLE_NAME, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Second"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(secondUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(SHARD_INDEX_TABLE_NAME, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Third"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(thirdUID));
                bw.addMutation(mutation);
                
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }
            
            try {
                
                // write the reverse index table:
                bw = con.createBatchWriter(SHARD_RINDEX_TABLE_NAME, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("First"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(firstUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(SHARD_RINDEX_TABLE_NAME, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Second"));
                mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                                range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(secondUID));
                bw.addMutation(mutation);
                
                bw = con.createBatchWriter(SHARD_RINDEX_TABLE_NAME, bwConfig);
                mutation = new Mutation(lcNoDiacriticsType.normalize("Third"));
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
                bw = con.createBatchWriter(SHARD_TABLE_NAME, bwConfig);
                mutation = new Mutation(shard);
                
                mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("First") + "\u0000" + datatype + "\u0000" + firstUID, columnVisibility,
                                timeStamp, emptyValue);
                
                mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("Second") + "\u0000" + datatype + "\u0000" + secondUID, columnVisibility,
                                timeStamp, emptyValue);
                
                mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("Third") + "\u0000" + datatype + "\u0000" + thirdUID, columnVisibility,
                                timeStamp, emptyValue);
                
                bw.addMutation(mutation);
            } finally {
                if (null != bw) {
                    bw.close();
                }
            }
            
            try {
                // write metadata table:
                bw = con.createBatchWriter(MODEL_TABLE_NAME, bwConfig);
                
                mutation = new Mutation("NAME");
                mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), columnVisibility, timeStamp, emptyValue);
                mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), columnVisibility,
                                timeStamp, emptyValue);
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
