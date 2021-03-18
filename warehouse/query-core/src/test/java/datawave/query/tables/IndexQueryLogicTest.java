package datawave.query.tables;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.QueryTestTableHelper;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.QueryLogicTestHarness;
import datawave.query.testframework.cardata.CarsDataType;
import datawave.query.testframework.cardata.CarsDataType.CarField;
import datawave.query.testframework.cardata.GenericCarFields;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * See {@link GenericCarFields#index} for which fields are indexed in the data set used by this test.
 *
 * Also see {@link GenericCarFields#reverse} for reverse indices.
 */
public class IndexQueryLogicTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(IndexQueryLogicTest.class);
    
    public IndexQueryLogicTest() {
        super(CarsDataType.getManager());
    }
    
    @BeforeClass
    public static void setupClass() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCarFields();
        dataTypes.add(new CarsDataType(CarsDataType.CarEntry.tesla, generic));
        dataTypes.add(new CarsDataType(CarsDataType.CarEntry.ford, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    @Before
    public void querySetUp() throws IOException {
        log.debug("---------  querySetUp  ---------");
        
        // Super call to pick up authSet initialization
        super.querySetUp();
        
        this.logic = new IndexQueryLogic();
        QueryTestTableHelper.configureLogicToScanTables(this.logic);
        
        this.logic.setFullTableScanEnabled(false);
        this.logic.setIncludeDataTypeAsField(true);
        this.logic.setIncludeGroupingContext(true);
        
        this.logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        this.logic.setMarkingFunctions(new MarkingFunctions.Default());
        this.logic.setMetadataHelperFactory(new MetadataHelperFactory());
        this.logic.setQueryPlanner(new DefaultQueryPlanner());
        this.logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        
        // init must set auths
        testInit();
        
        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        DatawaveUser user = new DatawaveUser(dn, DatawaveUser.UserType.USER, Sets.newHashSet(this.auths.toString().split(",")), null, null, -1L);
        this.principal = new DatawavePrincipal(Collections.singleton(user));
        
        this.testHarness = new QueryLogicTestHarness(this);
    }
    
    @Test
    public void testQuery001() throws Exception {
        log.info("------ Test a AND b ------");
        
        Set<String> expected = new HashSet<>(2);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-002");
        
        String query = CarField.COLOR.name() + " == 'blue' and " + CarField.WHEELS.name() + " == '4'";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery002() throws Exception {
        log.info("------ Test a* AND b ------");
        
        Set<String> expected = new HashSet<>(2);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-002");
        
        String query = CarField.COLOR.name() + " =~ 'bl.*' and " + CarField.WHEELS.name() + " == '4'";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery003() throws Exception {
        log.info("------ Test a*.* AND b ------");
        
        Set<String> expected = new HashSet<>();
        
        String query = CarField.COLOR.name() + " =~ 'bl.*s.*' and " + CarField.WHEELS.name() + " == '4'";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery004() throws Exception {
        log.info("------ Test !a AND b ------");
        
        Set<String> expected = new HashSet<>(4);
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-003");
        
        String query = CarField.COLOR.name() + " != 'blue' and " + CarField.WHEELS.name() + " == '4'";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery005() throws Exception {
        log.info("------ Test a AND NOT (b) ------");
        
        Set<String> expected = new HashSet<>(4);
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-003");
        
        String query = CarField.WHEELS.name() + " == '4' and not (" + CarField.COLOR.name() + " == 'blue')";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery006() throws Exception {
        log.info("------ Test ( a AND b ) OR ( c AND d) ------");
        
        Set<String> expected = new HashSet<>(3);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-002");
        
        String query = "(" + CarField.WHEELS.name() + " == '4' and " + CarField.COLOR.name() + " == 'blue')" + " or (" + CarField.MODEL + ") == 'models' and "
                        + CarField.COLOR.name() + " == 'red'";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery007() throws Exception {
        log.info("------ Test (a AND b AND !c) OR (d AND e) ------");
        
        Set<String> expected = new HashSet<>(3);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-002");
        
        String query = "(" + CarField.WHEELS.name() + " == '4' and " + CarField.COLOR.name() + " == 'blue' and " + CarField.MODEL.name()
                        + " != 'catamaran') or " + "(" + CarField.MODEL.name() + " == 'models' and " + CarField.COLOR.name() + " == 'red')";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery008() throws Exception {
        log.info("------ Test (a AND b AND !c) OR (d AND e) OR (f) ------");
        
        Set<String> expected = new HashSet<>(3);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-002");
        
        String query = "(" + CarField.WHEELS.name() + " == '4' and " + CarField.COLOR.name() + " == 'blue' and " + CarField.MODEL.name()
                        + " != 'catamaran') or " + "(" + CarField.MODEL.name() + " == 'models' and " + CarField.COLOR.name() + " == 'red') or " + "("
                        + CarField.WHEELS.name() + " == '3')";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery009() throws Exception {
        log.info("------ Test a OR b ------");
        
        Set<String> expected = new HashSet<>(2);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-002");
        
        String query = CarField.WHEELS.name() + "=='3' or " + CarField.COLOR.name() + "=='blue'";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery010() throws Exception {
        log.info("------ Test (a OR b) AND !c ------");
        
        Set<String> expected = new HashSet<>(3);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-002");
        
        String query = "(" + CarField.COLOR.name() + " == 'blue' or " + CarField.COLOR.name() + " == 'red') and " + CarField.MAKE.name() + "!='car'";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery011() throws Exception {
        log.info("------ Test (a OR b AND !c) or (d) ------");
        
        Set<String> expected = new HashSet<>(3);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-002");
        
        String query = "(" + CarField.COLOR.name() + "=='blue' or " + CarField.COLOR.name() + "=='red' and " + CarField.MAKE.name() + " != 'ford') or " + "("
                        + CarField.COLOR.name() + "=='blue')";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery012() throws Exception {
        log.info("------ Test a AND NOT (b or c) ------");
        
        Set<String> expected = new HashSet<>(1);
        expected.add("tesla-eventid-002");
        
        String query = CarField.MAKE.name() + "=='tesla' and not (" + CarField.COLOR.name() + "=='yellow' or " + CarField.COLOR.name() + "=='red')";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery013() throws Exception {
        log.info("------ Test a> AND b< ------");
        
        Set<String> expected = new HashSet<>(6);
        expected.add("ford-eventid-001");
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-002");
        expected.add("tesla-eventid-003");
        
        String query = "((_Bounded_ = true) && (" + CarField.WHEELS.name() + ">='0' and " + CarField.WHEELS.name() + "<='5'))";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery014() throws Exception {
        log.info("------ Test a and b ------");
        
        Set<String> expected = new HashSet<>(1);
        expected.add("tesla-eventid-001");
        
        String query = CarField.COLOR.name() + " == 'red' and " + CarField.MAKE.name() + " == 'tesla'";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery015() throws Exception {
        log.info("------ Test a AND (c OR d) ------");
        
        Set<String> expected = new HashSet<>(3);
        expected.add("ford-eventid-001");
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        
        String query = CarField.MAKE.name() + " == 'ford' and (" + CarField.COLOR.name() + " == 'silver' or " + CarField.WHEELS + " == '4')";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery016() throws Exception {
        log.info("------ Test a OR b ------");
        
        Set<String> expected = new HashSet<>(3);
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-002");
        expected.add("tesla-eventid-003");
        
        String query = CarField.COLOR.name() + " == 'red' or " + CarField.MAKE.name() + " == 'tesla'";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery017() throws Exception {
        log.info("------ Test (a OR b) AND (!c) ------");
        
        Set<String> expected = new HashSet<>(3);
        expected.add("tesla-eventid-001");
        expected.add("tesla-eventid-002");
        expected.add("tesla-eventid-003");
        
        String query = "(" + CarField.COLOR.name() + " == 'red' or " + CarField.MAKE.name() + " == 'tesla' ) and " + "(" + CarField.MAKE.name() + " != 'Trek')";
        runTest(query, expected);
    }
    
    /**
     * Query should be throwing an InvalidFieldIndexQueryFatalException due to AND with a non-indexed field.
     *
     * @throws Exception
     */
    @Test
    public void testQuery018() throws Exception {
        log.info("------ Test (a OR b ) AND (c), c not indexed -----");
        
        Set<String> expected = new HashSet<>(4);
        expected.add("ford-eventid-001");
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        expected.add("tesla-eventid-002");
        
        String query = "(" + CarField.COLOR.name() + " == 'blue' or " + CarField.MAKE.name() + " == 'ford') and " + "(" + CarField.DESC.name()
                        + " =~ '.*common.*')";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery019() throws Exception {
        log.info("------ Test (a OR b OR c) AND (!d) ------");
        
        Set<String> expected = new HashSet<>(4);
        expected.add("ford-eventid-001");
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        expected.add("tesla-eventid-002");
        
        String query = "(" + CarField.COLOR.name() + " == 'blue' or " + CarField.MAKE.name() + " == 'ford' or " + CarField.COLOR.name() + " == 'orange') and ("
                        + CarField.MAKE.name() + " != 'other')";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery020() throws Exception {
        log.info("------ Test (a OR b OR (c AND d)) AND (!e) ------");
        
        Set<String> expected = new HashSet<>(4);
        expected.add("ford-eventid-001");
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        expected.add("tesla-eventid-002");
        
        String query = "(" + CarField.COLOR.name() + " == 'blue' or " + CarField.MAKE.name() + " == 'ford' or " + "(" + CarField.COLOR.name()
                        + " == 'orange' and " + CarField.MODEL.name() + " == 'models') ) and " + "(" + CarField.MAKE.name() + " != 'other')";
        runTest(query, expected);
    }
    
    @Test
    public void testQuery021() throws Exception {
        log.info("------ Test (a OR b OR (c AND d)) AND (!e) ------");
        
        Set<String> expected = new HashSet<>(4);
        expected.add("ford-eventid-001");
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        expected.add("tesla-eventid-002");
        
        String query = "(" + CarField.COLOR.name() + " == 'blue' or " + CarField.MAKE.name() + " == 'ford' or " + "(" + CarField.COLOR.name()
                        + " == 'orange' and " + CarField.DESC.name() + " == 'blank') ) and " + "(" + CarField.MAKE.name() + " != 'other')";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery022() throws Exception {
        log.info("------ Test (a AND b), where b is an unevaluated field ------");
        
        Set<String> expected = new HashSet<>(0);
        
        ArrayList<String> unevaluatedFields = Lists.newArrayList(CarField.MAKE.name());
        logic.setUnevaluatedFields(unevaluatedFields);
        
        String query = CarField.COLOR.name() + " == 'blue' and " + CarField.MAKE.name() + " == 'car'";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery023() throws Exception {
        log.info("------ Test (a AND !b), where b is an unevaluated field ------");
        
        Set<String> expected = new HashSet<>(2);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-002");
        
        ArrayList<String> unevaluatedFields = Lists.newArrayList(CarField.MAKE.name());
        logic.setUnevaluatedFields(unevaluatedFields);
        
        String query = CarField.COLOR.name() + " == 'blue' and !(" + CarField.MAKE.name() + " == 'car')";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery024() throws Exception {
        log.info("------ Test (a), where a is an unevaluated field ------");
        
        Set<String> expected = new HashSet<>(2);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-002");
        
        ArrayList<String> unevaluatedFields = Lists.newArrayList(CarField.COLOR.name());
        logic.setUnevaluatedFields(unevaluatedFields);
        
        String query = CarField.COLOR.name() + " == 'blue'";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery025() throws Exception {
        log.info("------ Test (a), where a is an un-fielded term ------");
        
        Set<String> expected = new HashSet<>(2);
        expected.add("ford-eventid-001");
        expected.add("tesla-eventid-002");
        
        String query = Constants.ANY_FIELD + " == 'blue'";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery026() throws Exception {
        log.info("------ Test (a AND b), where all terms are un-fielded ------");
        
        Set<String> expected = new HashSet<>(0);
        
        String query = "(" + Constants.ANY_FIELD + " == 'blue' and " + Constants.ANY_FIELD + " == 'car')";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery027() throws Exception {
        log.info("------ Test (a OR b), where all terms are un-fielded ------");
        
        Set<String> expected = new HashSet<>(4);
        expected.add("ford-eventid-001");
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        expected.add("tesla-eventid-002");
        
        String query = "(" + Constants.ANY_FIELD + " == 'blue' or " + Constants.ANY_FIELD + " == 'ford')";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery028() throws Exception {
        log.info("------ Test ((a OR b) AND c), where all terms are un-fielded ------");
        
        Set<String> expected = new HashSet<>(1);
        expected.add("tesla-eventid-002");
        
        String query = "(" + Constants.ANY_FIELD + " == 'blue' or " + Constants.ANY_FIELD + " == 'ford') and " + Constants.ANY_FIELD + " == 'tesla'";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery029() throws Exception {
        log.info("------ Test (a OR b OR c), where all terms are un-fielded and one term does not exist ------");
        
        Set<String> expected = new HashSet<>(4);
        expected.add("ford-eventid-001");
        expected.add("ford-eventid-002");
        expected.add("ford-eventid-003");
        expected.add("tesla-eventid-002");
        String query = "(" + Constants.ANY_FIELD + " == 'blue' or " + Constants.ANY_FIELD + " == 'ford' or " + Constants.ANY_FIELD
                        + " == 'termthatdoesnotexist')";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery030() throws Exception {
        log.info("------ Test (a AND b AND c), where all terms are un-fielded and one term does not exist ------");
        
        Set<String> expected = new HashSet<>(0);
        
        String query = "(" + Constants.ANY_FIELD + " == 'blue' and " + Constants.ANY_FIELD + " == 'ford' and " + Constants.ANY_FIELD
                        + " == 'termthatdoesnotexist')";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery031() throws Exception {
        log.info("------ Test (a), where a is a fielded regex ------");
        
        Set<String> expected = new HashSet<>(0);
        expected.add("tesla-eventid-001");
        
        String query = "(" + CarField.COLOR.name() + " =~ 're.*')";
        
        runTest(query, expected);
    }
    
    @Test
    public void testQuery032() throws Exception {
        log.info("------ Test (a), where a is an un-fielded regex ------");
        
        Set<String> expected = new HashSet<>(0);
        expected.add("tesla-eventid-001");
        
        String query = "(" + Constants.ANY_FIELD + " =~ 're.*')";
        
        runTest(query, expected);
    }
    
    public void runTest(String query, Set<String> expected) throws Exception {
        Date[] startEndDate = this.dataManager.getShardStartEndDate();
        Map<String,String> options = new HashMap<>();
        final List<QueryLogicTestHarness.DocumentChecker> queryChecker = new ArrayList<>();
        runTestQuery(expected, query, startEndDate[0], startEndDate[1], options, queryChecker);
    }
    
    @Override
    protected void testInit() {
        this.auths = CarsDataType.getTestAuths();
        this.documentKey = CarsDataType.CarField.EVENT_ID.name();
    }
}
