package datawave.query.testframework;

import com.google.common.collect.Sets;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions.NoOp;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.CountingShardQueryLogic;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.content.ContentQueryTable;
import datawave.query.testframework.QueryLogicTestHarness.DocumentChecker;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

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
    
    private static final Logger log = Logger.getLogger(AbstractFunctionalQuery.class);
    
    private static final Random rVal = new Random(System.currentTimeMillis());
    
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
    private QueryLogicTestHarness testHarness;
    protected DatawavePrincipal principal;
    
    @Rule
    public final TemporaryFolder tmpDir = new TemporaryFolder();
    
    private final Set<Authorizations> authSet = new HashSet<>();
    
    protected AbstractFunctionalQuery(final RawDataManager mgr) {
        this.dataManager = mgr;
    }
    
    @Before
    public void querySetUp() {
        log.debug("---------  querySetUp  ---------");
        
        this.logic = new ShardQueryLogic();
        QueryTestTableHelper.configureLogicToScanTables(this.logic);
        
        this.logic.setFullTableScanEnabled(false);
        this.logic.setIncludeDataTypeAsField(true);
        this.logic.setIncludeGroupingContext(true);
        
        this.logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        this.logic.setMarkingFunctions(new NoOp());
        this.logic.setMetadataHelperFactory(new MetadataHelperFactory());
        this.logic.setQueryPlanner(new DefaultQueryPlanner());
        this.logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        
        CountingShardQueryLogic countLogic = new CountingShardQueryLogic();
        QueryTestTableHelper.configureLogicToScanTables(countLogic);
        
        countLogic.setIncludeDataTypeAsField(true);
        countLogic.setIncludeGroupingContext(true);
        countLogic.setFullTableScanEnabled(false);
        
        countLogic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        countLogic.setMarkingFunctions(new NoOp());
        countLogic.setMetadataHelperFactory(new MetadataHelperFactory());
        countLogic.setQueryPlanner(new DefaultQueryPlanner());
        countLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        
        ContentQueryTable contentLogic = new ContentQueryTable();
        contentLogic.setMaxResults(5000);
        contentLogic.setMaxRowsToScan(25000);
        contentLogic.setTableName(QueryTestTableHelper.SHARD_TABLE_NAME);
        contentLogic.setMarkingFunctions(new NoOp());
        contentLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        
        // init must set auths
        testInit();
        
        Assert.assertNotNull(this.auths);
        authSet.clear();
        authSet.add(this.auths);
        
        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        DatawaveUser user = new DatawaveUser(dn, DatawaveUser.UserType.USER, Sets.newHashSet(this.auths.toString().split(",")), null, null, -1L);
        this.principal = new DatawavePrincipal(Collections.singleton(user));
        
        QueryLogicTestHarness.TestResultParser resp = (key, document) -> {
            return this.parse(key, document);
        };
        this.testHarness = new QueryLogicTestHarness(this);
    }
    
    // ============================================
    // abstract methods
    
    /**
     * Performs any instance initialization required for the specific test case.
     */
    abstract protected void testInit();
    
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
        runTest(query, expectQuery, Collections.EMPTY_MAP);
    }
    
    protected void runTest(final String query, final String expectQuery, final Date startDate, final Date endDate) throws Exception {
        QueryJexl jexl = new QueryJexl(expectQuery, this.dataManager, startDate, endDate);
        final Set<Map<String,String>> allData = jexl.evaluate();
        final Set<String> expected = this.dataManager.getKeys(allData);
        runTestQuery(expected, expectQuery, startDate, endDate);
    }
    
    protected void runTest(final String query, final String expectQuery, final Map<String,String> options) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        
        QueryJexl jexl = new QueryJexl(expectQuery, this.dataManager, startEndDate[0], startEndDate[1]);
        final Set<Map<String,String>> allData = jexl.evaluate();
        final Set<String> expected = this.dataManager.getKeys(allData);
        
        runTestQuery(expected, query, startEndDate[0], startEndDate[1], options);
    }
    
    protected void runTest(final String query, final String expectQuery, final Map<String,String> options, final List<DocumentChecker> checkers)
                    throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        
        QueryJexl jexl = new QueryJexl(expectQuery, this.dataManager, startEndDate[0], startEndDate[1]);
        final Set<Map<String,String>> allData = jexl.evaluate();
        final Set<String> expected = this.dataManager.getKeys(allData);
        
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
        log.debug("  query[" + queryStr + "]  start(" + YMD_DateFormat.format(startDate) + ")  end(" + YMD_DateFormat.format(endDate) + ")");
        QueryImpl q = new QueryImpl();
        q.setBeginDate(startDate);
        q.setEndDate(endDate);
        q.setQuery(queryStr);
        q.setParameters(options);
        
        q.setId(UUID.randomUUID());
        q.setPagesize(Integer.MAX_VALUE);
        q.setQueryAuthorizations(auths.toString());
        
        GenericQueryConfiguration config = this.logic.initialize(connector, q, this.authSet);
        this.logic.setupQuery(config);
        testHarness.assertLogicResults(this.logic, expected, checkers);
    }
    
    /**
     * Configures the Ivarator cache to use a single HDFS directory.
     *
     * @throws IOException
     *             error creating cache
     */
    protected void ivaratorConfig() throws IOException {
        ivaratorConfig(1, false);
    }
    
    /**
     * Configures an Ivarator FST cache to use for a single HDFS directory.
     * 
     * @throws IOException
     *             error creating cache
     */
    protected void ivaratorFstConfig() throws IOException {
        ivaratorConfig(1, true);
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
    protected void ivaratorConfig(final int hdfsLocations, final boolean fst) throws IOException {
        final URL hdfsConfig = this.getClass().getResource("/testhadoop.config");
        Assert.assertNotNull(hdfsConfig);
        this.logic.setHdfsSiteConfigURLs(hdfsConfig.toExternalForm());
        
        final String fstCache = (fst ? "fst-" : "");
        final List<String> dirs = new ArrayList<>();
        for (int d = 1; d <= hdfsLocations; d++) {
            // avoid threading issues by using a random number as part of the directory path to create a distinct directory
            int rand = rVal.nextInt(Integer.MAX_VALUE);
            File ivCache = this.tmpDir.newFolder("ivarator.cache-" + fstCache + rand);
            dirs.add(ivCache.toURI().toString());
        }
        String uriList = String.join(",", dirs);
        log.info("hdfs dirs(" + uriList + ")");
        if (fst) {
            this.logic.setIvaratorFstHdfsBaseURIs(uriList);
        } else {
            this.logic.setIvaratorCacheBaseURIs(uriList);
        }
    }
}
