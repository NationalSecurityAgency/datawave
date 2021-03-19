package datawave.query;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

public abstract class IvaratorInterruptTest {
    private static final Logger log = Logger.getLogger(IvaratorInterruptTest.class);
    private static Connector connector;
    
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    protected Authorizations auths = new Authorizations("ALL");
    private Set<Authorizations> authSet = Collections.singleton(auths);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    
    private KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @Deployment
    public static JavaArchive createDeployment() {
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class)
                        .deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    protected static void init(String metadataDir, WiseGuysIngest.WhatKindaRange range) throws Exception {
        System.setProperty("type.metadata.dir", temporaryFolder.getRoot().getCanonicalPath() + "/" + metadataDir);
        
        QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log, RebuildingScannerTestHelper.TEARDOWN.NEVER,
                        RebuildingScannerTestHelper.INTERRUPT.FI_EVERY_OTHER);
        connector = qtth.connector;
        
        WiseGuysIngest.writeItAll(connector, range);
        Authorizations auths = new Authorizations("ALL");
        PrintUtility.printTable(connector, auths, TableName.SHARD);
        PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }
    
    @AfterClass
    public static void teardown() throws IOException {
        TypeRegistry.reset();
        
        File root = new File(temporaryFolder.getRoot().getCanonicalPath() + "/DocumentRange/DatawaveMetadata");
        FilenameFilter filenameFilter = (dir, name) -> name.equals("typeMetadata") || name.equals("typeMetadata.crc");
        if (root.exists()) {
            for (File file : root.listFiles(filenameFilter)) {
                System.out.println("Deleting file " + file.getCanonicalPath() + " : " + file.delete());
            }
        } else {
            System.out.println("Root doesn't exist");
        }
    }
    
    @Before
    public void setup() throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        logic.setFullTableScanEnabled(true);
        // this should force regex expansion into ivarators
        logic.setMaxValueExpansionThreshold(1);
        
        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());
        
        // setup a directory for cache results
        File tmpDir = temporaryFolder.newFolder();
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(tmpDir.toURI().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));
        
        deserializer = new KryoDocumentDeserializer();
    }
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends IvaratorInterruptTest {
        
        @BeforeClass
        public static void init() throws Exception {
            init(ShardRange.class.getSimpleName(), WiseGuysIngest.WhatKindaRange.SHARD);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends IvaratorInterruptTest {
        
        @BeforeClass
        public static void init() throws Exception {
            init(DocumentRange.class.getSimpleName(), WiseGuysIngest.WhatKindaRange.DOCUMENT);
        }
    }
    
    protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
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
        
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());
        logic.setMaxEvaluationPipelines(1);
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        
        HashSet<String> expectedSet = new HashSet<>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<>();
        Set<Document> docs = new HashSet<>();
        for (Map.Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            
            log.debug(entry.getKey() + " => " + d);
            
            Attribute<?> attr = d.get("UUID");
            if (attr == null)
                attr = d.get("UUID.0");
            
            Assert.assertNotNull("Result Document did not contain a 'UUID'", attr);
            Assert.assertTrue("Expected result to be an instance of DatwawaveTypeAttribute, was: " + attr.getClass().getName(), attr instanceof TypeAttribute
                            || attr instanceof PreNormalizedAttribute);
            
            TypeAttribute<?> UUIDAttr = (TypeAttribute<?>) attr;
            
            String UUID = UUIDAttr.getType().getDelegate().toString();
            Assert.assertTrue("Received unexpected UUID: " + UUID, expected.contains(UUID));
            
            resultSet.add(UUID);
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
    }
    
    @Test
    public void testIvaratorInterruptedUnsorted() throws Exception {
        String query = "UUID =~ '^[CS].*'";
        String[] results = new String[] {"CORLEONE", "SOPRANO", "CAPONE"};
        runTestQuery(Arrays.asList(results), query, format.parse("20091231"), format.parse("20150101"), Collections.emptyMap());
    }
    
    @Test
    public void testIvaratorInterruptedSorted() throws Exception {
        Map<String,String> params = new HashMap<>();
        
        // both required in order to force ivarator to call fillSets
        params.put(QueryOptions.SORTED_UIDS, "true");
        logic.getConfig().setUnsortedUIDsEnabled(false);
        
        String query = "UUID =~ '^[CS].*'";
        String[] results = new String[] {"CORLEONE", "SOPRANO", "CAPONE"};
        runTestQuery(Arrays.asList(results), query, format.parse("20091231"), format.parse("20150101"), params);
    }
}
