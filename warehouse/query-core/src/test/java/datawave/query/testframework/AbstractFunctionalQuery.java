package datawave.query.testframework;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions.Default;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.CountingShardQueryLogic;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.testframework.QueryLogicTestHarness.DocumentChecker;
import datawave.query.transformer.ShardQueryCountTableTransformer;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.cache.QueryMetricFactory;
import datawave.webservice.query.cache.QueryMetricFactoryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides the basic initialization required to initialize and execute queries. This class will initialize the following runtime settings:
 * <ul>
 * <li>timezone => GMT</li>
 * <li>file.encoding => UTF-8</li>
 * <li>DATAWAVE_INGEST_HOME => target directory</li>
 * <li>hadoop.home.dir => target directory</li>
 * </ul>
 */
public abstract class AbstractFunctionalQuery implements QueryLogicTestHarness.TestResultParser {
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    protected static final String VALUE_THRESHOLD_JEXL_NODE = ExceededValueThresholdMarkerJexlNode.label();
    protected static final String FILTER_EXCLUDE_REGEX = "filter:excludeRegex";
    
    private static final Logger log = Logger.getLogger(AbstractFunctionalQuery.class);
    
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", StandardCharsets.UTF_8.name());
        System.setProperty(DnUtils.NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        try {
            File dir = new File(ClassLoader.getSystemClassLoader().getResource(".").toURI());
            File targetDir = dir.getParentFile();
            System.setProperty("DATAWAVE_INGEST_HOME", targetDir.getAbsolutePath());
            System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());
        } catch (URISyntaxException se) {
            log.error("failed to get URI for .", se);
            Assert.fail();
        }
    }
    
    private boolean useRunningQuery = false;
    private QueryMetricFactory metricFactory;
    
    /**
     * Contains a list of cities that are specified in the test data. Additional cities can be added to the test data and do not specifically need to be added
     * here. The purpose is to provide a location where the city names are specified without having to hard code these entries throughout the test cases.
     */
    public enum TestCities {
        // any city entries can be added; these exist in the current set of data
        london,
        paris,
        rome;
    }
    
    private static final SimpleDateFormat YMD_DateFormat = new SimpleDateFormat("yyyyMMdd");
    
    // ============================================
    // static members
    protected static Connector connector;
    
    // ============================================
    // instance members
    /**
     * Manager for the raw data should exist in Accumulo.
     */
    protected final RawDataManager dataManager;
    protected Authorizations auths;
    protected String documentKey;
    protected ShardQueryLogic logic;
    private CountingShardQueryLogic countLogic = new CountingShardQueryLogic();
    protected QueryLogicTestHarness testHarness;
    protected DatawavePrincipal principal;
    
    private final Set<Authorizations> authSet = new HashSet<>();
    
    protected AbstractFunctionalQuery(final RawDataManager mgr) {
        this.dataManager = mgr;
    }
    
    protected ShardQueryLogic createQueryLogic() {
        return new ShardQueryLogic();
    }
    
    @Before
    public void querySetUp() throws IOException {
        log.debug("---------  querySetUp  ---------");
        
        this.logic = createQueryLogic();
        QueryTestTableHelper.configureLogicToScanTables(this.logic);
        
        this.logic.setFullTableScanEnabled(false);
        this.logic.setIncludeDataTypeAsField(true);
        
        this.logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        this.logic.setMarkingFunctions(new Default());
        this.logic.setMetadataHelperFactory(new MetadataHelperFactory());
        this.logic.setQueryPlanner(new DefaultQueryPlanner());
        this.logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        
        this.logic.setCollectTimingDetails(true);
        this.logic.setLogTimingDetails(true);
        this.logic.setMinimumSelectivity(0.03D);
        this.logic.setMaxIndexScanTimeMillis(5000);
        
        // count logic
        countLogic.setIncludeDataTypeAsField(true);
        countLogic.setFullTableScanEnabled(false);
        
        countLogic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        countLogic.setMarkingFunctions(new Default());
        countLogic.setMetadataHelperFactory(new MetadataHelperFactory());
        countLogic.setQueryPlanner(new DefaultQueryPlanner());
        countLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        
        QueryTestTableHelper.configureLogicToScanTables(countLogic);
        
        // init must set auths
        testInit();
        
        Assert.assertNotNull(this.auths);
        authSet.clear();
        authSet.add(this.auths);
        
        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        DatawaveUser user = new DatawaveUser(dn, DatawaveUser.UserType.USER, Sets.newHashSet(this.auths.toString().split(",")), null, null, -1L);
        this.principal = new DatawavePrincipal(Collections.singleton(user));
        this.testHarness = new QueryLogicTestHarness(this);
    }
    
    // ============================================
    // abstract methods
    
    /**
     * Performs any instance initialization required for the specific test case.
     */
    protected abstract void testInit();
    
    public void debugQuery() {
        Logger.getRootLogger().setLevel(Level.DEBUG);
        Logger.getLogger("datawave.query").setLevel(Level.DEBUG);
        Logger.getLogger("datawave.query.planner").setLevel(Level.DEBUG);
        Logger.getLogger("datawave.query.planner.DefaultQueryPlanner").setLevel(Level.DEBUG);
    }
    
    // ============================================
    // implementation interface methods
    @Override
    public String parse(Key key, Document document) {
        Attribute<?> attr = document.get(this.documentKey);
        Assert.assertNotNull("document key(" + this.documentKey + ") attribute is null", attr);
        
        Object data = attr.getData();
        Assert.assertNotNull("document key attribute is null: key(" + this.documentKey + ")", data);
        String keyVal = null;
        if (data instanceof Type) {
            keyVal = ((Type<?>) data).getDelegate().toString();
        } else if (data instanceof String) {
            keyVal = (String) data;
        } else {
            Assert.fail("invalid type for document key(" + this.documentKey + "");
        }
        
        return keyVal;
    }
    
    // ============================================
    // basic test execution methods
    
    /**
     * This method should be called to determine if a marker node exists in the generated query, such as a ExceededValueThresholdMarkerJexlNode or
     * ASTDelayedPredicate.
     * 
     * @param subStr
     *            substring to find in the plan (maker node class name)
     * @param expect
     *            number of instances to find in the plan (use 0 for exclusion)
     */
    protected void parsePlan(String subStr, int expect) {
        ShardQueryConfiguration config = this.logic.getConfig();
        String plan = config.getQueryString();
        int idx;
        int total = 0;
        String test = plan;
        do {
            idx = test.indexOf(subStr);
            if (-1 < idx) {
                total++;
                test = test.substring(idx + subStr.length());
            }
        } while (-1 < idx);
        Assert.assertEquals("marker (" + subStr + ") in plan: " + plan, expect, total);
    }
    
    /**
     * Helper method for determining the expected results for a query.
     *
     * @param query
     *            query for evaluation
     * @return expected results based upon the defined key for the datatype
     */
    protected Collection<String> getExpectedKeyResponse(final String query) {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        return getExpectedKeyResponse(query, startEndDate[0], startEndDate[1]);
    }
    
    /**
     * Helper method for determining the expected results for a query.
     *
     * @param query
     *            query for evaluation
     * @param start
     *            start date for evaluation
     * @param end
     *            end date for evaluation
     * @return expected results based upon the defined key for the datatype
     */
    protected Collection<String> getExpectedKeyResponse(final String query, final Date start, final Date end) {
        QueryJexl jexl = new QueryJexl(query, this.dataManager, start, end);
        final Set<Map<String,String>> allData = jexl.evaluate();
        return this.dataManager.getKeys(allData);
    }
    
    /**
     * Base test execution. This will use the first and last shard dates for the date range.
     *
     * @param query
     *            query for execution
     * @param expectQuery
     *            query for expected results
     * @throws Exception
     *             test error condition
     */
    protected void runTest(final String query, final String expectQuery) throws Exception {
        runTest(query, expectQuery, (Map<String,String>) Collections.EMPTY_MAP);
    }
    
    protected void runTest(final String query, final Collection<String> expectResp) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        runTestQuery(expectResp, query, startEndDate[0], startEndDate[1]);
    }
    
    protected void runTest(final String query, final String expectQuery, final Date startDate, final Date endDate) throws Exception {
        final Collection<String> expected = getExpectedKeyResponse(expectQuery, startDate, endDate);
        runTestQuery(expected, query, startDate, endDate);
    }
    
    protected void runTest(final String query, final String expectQuery, final Map<String,String> options) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        final Collection<String> expected = getExpectedKeyResponse(expectQuery);
        runTestQuery(expected, query, startEndDate[0], startEndDate[1], options);
    }
    
    protected void runTest(final String query, final String expectQuery, final Map<String,String> options, final List<DocumentChecker> checkers)
                    throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        final Collection<String> expected = getExpectedKeyResponse(expectQuery);
        runTestQuery(expected, query, startEndDate[0], startEndDate[1], options, checkers);
    }
    
    /**
     * Equivalent to {@link #runTestQuery(Collection, String, Date, Date, Map)}, with an empty map for the options.
     *
     * @param expected
     *            expected results from the query
     * @param queryStr
     *            execution query string
     * @param startDate
     *            start date for query (inclusive)
     * @param endDate
     *            end date for query (exclusive)
     */
    protected void runTestQuery(Collection<String> expected, String queryStr, Date startDate, Date endDate) throws Exception {
        runTestQuery(expected, queryStr, startDate, endDate, Collections.emptyMap());
    }
    
    protected void runTestQuery(Collection<String> expected, String queryStr) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        runTestQuery(expected, queryStr, startEndDate[0], startEndDate[1], Collections.emptyMap());
    }
    
    /**
     * Equivalent to {@link #runTestQuery(Collection, String, Date, Date, Map, List)}, with an empty list for the checkers.
     *
     * @param expected
     *            expected results from the query
     * @param queryStr
     *            execution query string
     * @param startDate
     *            start date for query (inclusive)
     * @param endDate
     *            end date for query (exclusive)
     * @param options
     *            optional parameters to query
     */
    protected void runTestQuery(Collection<String> expected, String queryStr, Date startDate, Date endDate, Map<String,String> options) throws Exception {
        runTestQuery(expected, queryStr, startDate, endDate, options, Collections.emptyList());
    }
    
    /**
     * Executes the query and performs validation of the results.
     *
     * @param expected
     *            expected results from the query
     * @param queryStr
     *            execution query string
     * @param startDate
     *            start date for query (inclusive)
     * @param endDate
     *            end date for query (exclusive)
     * @param options
     *            optional parameters to query
     * @param checkers
     *            optional list of assert checker methods
     */
    protected void runTestQuery(Collection<String> expected, String queryStr, Date startDate, Date endDate, Map<String,String> options,
                    List<DocumentChecker> checkers) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("  query[" + queryStr + "]  start(" + YMD_DateFormat.format(startDate) + ")  end(" + YMD_DateFormat.format(endDate) + ")");
        }
        QueryImpl q = new QueryImpl();
        q.setBeginDate(startDate);
        q.setEndDate(endDate);
        q.setQuery(queryStr);
        q.setParameters(options);
        
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());
        if (useRunningQuery) {
            QueryMetricFactory queryMetricFactory = (metricFactory == null) ? new QueryMetricFactoryImpl() : metricFactory;
            new RunningQuery(connector, AccumuloConnectionFactory.Priority.NORMAL, this.logic, q, "", principal, queryMetricFactory);
        } else {
            GenericQueryConfiguration config = this.logic.initialize(connector, q, this.authSet);
            this.logic.setupQuery(config);
            if (log.isDebugEnabled()) {
                log.debug("Plan: " + config.getQueryString());
            }
        }
        testHarness.assertLogicResults(this.logic, expected, checkers);
    }
    
    /**
     * Executes test cases that use {@link CountingShardQueryLogic}.
     *
     * @param query
     *            query for evaluation
     * @throws Exception
     *             error condition during execution of query
     */
    protected void runCountTest(String query) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        if (log.isDebugEnabled()) {
            log.debug("  count query[" + query + "]  start(" + YMD_DateFormat.format(startEndDate[0]) + ")  end(" + YMD_DateFormat.format(startEndDate[1])
                            + ")");
        }
        
        QueryImpl q = new QueryImpl();
        q.setBeginDate(startEndDate[0]);
        q.setEndDate(startEndDate[1]);
        q.setQuery(query);
        
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());
        
        RunningQuery runner = new RunningQuery(connector, AccumuloConnectionFactory.Priority.NORMAL, this.countLogic, q, "", principal,
                        new QueryMetricFactoryImpl());
        TransformIterator it = runner.getTransformIterator();
        ShardQueryCountTableTransformer ctt = (ShardQueryCountTableTransformer) it.getTransformer();
        EventQueryResponseBase resp = (EventQueryResponseBase) ctt.createResponse(runner.next());
        
        Collection<String> expect = getExpectedKeyResponse(query);
        
        List<EventBase> events = resp.getEvents();
        Assert.assertEquals(1, events.size());
        EventBase<?,?> event = events.get(0);
        List<?> fields = event.getFields();
        Assert.assertEquals(1, fields.size());
        FieldBase<?> count = (FieldBase) fields.get(0);
        String val = count.getValueString();
        if (log.isDebugEnabled()) {
            log.debug("expected count(" + expect.size() + ") actual count(" + val + ")");
        }
        Assert.assertEquals("" + expect.size(), val);
    }
    
    /**
     * Used by test cases that verify the configuration.
     *
     * @param queryStr
     *            query string for evaluation
     * @return query configuration
     * @throws Exception
     *             error condition from query initialization
     */
    protected GenericQueryConfiguration setupConfig(final String queryStr) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        if (log.isDebugEnabled()) {
            log.debug("  query[" + queryStr + "]  start(" + YMD_DateFormat.format(startEndDate[0]) + ")  end(" + YMD_DateFormat.format(startEndDate[1]) + ")");
        }
        
        QueryImpl q = new QueryImpl();
        q.setBeginDate(startEndDate[0]);
        q.setEndDate(startEndDate[1]);
        q.setQuery(queryStr);
        
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());
        
        return this.logic.initialize(connector, q, this.authSet);
    }
    
    /**
     * Used by test cases that verify the plan
     *
     * @param queryStr
     *            query string for evaluation
     * @param expandFields
     *            whether to use the index for anyfield expansion
     * @param expandValues
     *            whether to use the index for regex/range expansion
     * @return query configuration
     * @throws Exception
     *             error condition from query initialization
     */
    protected String getPlan(final String queryStr, boolean expandFields, boolean expandValues) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        if (log.isDebugEnabled()) {
            log.debug("  query[" + queryStr + "]  start(" + YMD_DateFormat.format(startEndDate[0]) + ")  end(" + YMD_DateFormat.format(startEndDate[1]) + ")");
        }
        
        QueryImpl q = new QueryImpl();
        q.setBeginDate(startEndDate[0]);
        q.setEndDate(startEndDate[1]);
        q.setQuery(queryStr);
        
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());
        
        return this.logic.getPlan(connector, q, this.authSet, expandFields, expandValues);
    }
    
    /**
     * Configures the Ivarator cache to use a single HDFS directory.
     *
     * @throws IOException
     *             error creating cache
     */
    protected List<String> ivaratorConfig() throws IOException {
        return ivaratorConfig(1, false)[0];
    }
    
    /**
     * Configures an Ivarator FST cache to use for a single HDFS directory.
     *
     * @throws IOException
     *             error creating cache
     */
    protected List<String>[] ivaratorFstConfig() throws IOException {
        return ivaratorConfig(1, true);
    }
    
    /**
     * Configures the Ivarator cache. Each cache directory is created as a separate directory.
     *
     * @param hdfsLocations
     *            number of HDFS locations to configure
     * @param fst
     *            when true screate a FST ivarator cache
     * @throws IOException
     *             error creating HDFS cache directory
     */
    protected List<String>[] ivaratorConfig(final int hdfsLocations, final boolean fst) throws IOException {
        final URL hdfsConfig = this.getClass().getResource("/testhadoop.config");
        Assert.assertNotNull(hdfsConfig);
        this.logic.setHdfsSiteConfigURLs(hdfsConfig.toExternalForm());
        
        final List<String> dirs = new ArrayList<>();
        final List<String> fstDirs = new ArrayList<>();
        for (int d = 1; d <= hdfsLocations; d++) {
            Path ivCache = Paths.get(temporaryFolder.newFolder().toURI());
            dirs.add(ivCache.toUri().toString());
            if (fst) {
                ivCache = Paths.get(temporaryFolder.newFolder().toURI());
                fstDirs.add(ivCache.toAbsolutePath().toString());
            }
        }
        String uriList = String.join(",", dirs);
        log.info("hdfs dirs(" + uriList + ")");
        this.logic.setIvaratorCacheDirConfigs(dirs.stream().map(IvaratorCacheDirConfig::new).collect(Collectors.toList()));
        if (fst) {
            uriList = String.join(",", fstDirs);
            log.info("fst dirs(" + uriList + ")");
            this.logic.setIvaratorFstHdfsBaseURIs(uriList);
        }
        
        return new List[] {dirs, fstDirs};
    }
    
    /**
     * assertQuery is almost the same as Assert.assertEquals except that it will allow for different orderings of the terms within an AND or and OR.
     * 
     * @param expected
     *            The expected query
     * @param query
     *            The query being tested
     */
    protected void assertPlanEquals(String expected, String query) throws ParseException {
        // first do the quick check
        if (expected.equals(query)) {
            return;
        }
        
        ASTJexlScript expectedTree = JexlASTHelper.parseJexlQuery(expected);
        expectedTree = TreeFlatteningRebuildingVisitor.flattenAll(expectedTree);
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(query);
        queryTree = TreeFlatteningRebuildingVisitor.flattenAll(queryTree);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedTree, queryTree);
        if (!comparison.isEqual()) {
            throw new ComparisonFailure(comparison.getReason(), expected, query);
        }
    }
    
    protected Multimap<String,Key> removeMetadataEntries(Set<String> fields, Text cf) throws AccumuloSecurityException, AccumuloException,
                    TableNotFoundException {
        Multimap<String,Key> metadataEntries = HashMultimap.create();
        MultiTableBatchWriter multiTableWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        BatchWriter writer = multiTableWriter.getBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME);
        for (String field : fields) {
            Mutation mutation = new Mutation(new Text(field));
            Scanner scanner = connector.createScanner(QueryTestTableHelper.METADATA_TABLE_NAME, new Authorizations());
            scanner.fetchColumnFamily(cf);
            scanner.setRange(new Range(new Text(field)));
            boolean foundEntries = false;
            for (Map.Entry<Key,Value> entry : scanner) {
                foundEntries = true;
                metadataEntries.put(field, entry.getKey());
                mutation.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(), entry.getKey().getColumnVisibilityParsed());
            }
            scanner.close();
            if (foundEntries) {
                writer.addMutation(mutation);
            }
        }
        writer.close();
        connector.tableOperations().compact(QueryTestTableHelper.METADATA_TABLE_NAME, new Text("\0"), new Text("~"), true, true);
        return metadataEntries;
    }
    
    protected void addMetadataEntries(Multimap<String,Key> metadataEntries) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        MultiTableBatchWriter multiTableWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        BatchWriter writer = multiTableWriter.getBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME);
        for (String field : metadataEntries.keySet()) {
            Mutation mutation = new Mutation(new Text(field));
            for (Key key : metadataEntries.get(field)) {
                metadataEntries.put(field, key);
                mutation.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), new Value());
            }
            writer.addMutation(mutation);
        }
        writer.close();
        connector.tableOperations().compact(QueryTestTableHelper.METADATA_TABLE_NAME, new Text("\0"), new Text("~"), true, true);
    }
    
    /**
     * The test framework typically just calls logic.initialize and logic.setupQuery. The typical code path for a query, as seen in RunningQuery, involves more.
     * This method will ensure that RunningQuery is used to more fully exercise the query.
     */
    protected void useRunningQuery() {
        this.useRunningQuery = true;
    }
    
    /**
     * When provided, the QueryMetric object will be used for running the query and so can be later inspected. Also see #useRunningQuery()
     * 
     * @param metric
     */
    protected void withMetric(BaseQueryMetric metric) {
        this.metricFactory = () -> metric;
    }
}
