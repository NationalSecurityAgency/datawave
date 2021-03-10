package datawave.query;

import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GroupsDataType;
import datawave.query.testframework.GroupsDataType.GroupField;
import datawave.query.testframework.GroupsDataType.GroupsEntry;
import datawave.query.testframework.GroupsIndexConfiguration;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GT_OP;
import static datawave.query.testframework.RawDataManager.LT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

public class GroupsQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(GroupsQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig fields = new GroupsIndexConfiguration();
        dataTypes.add(new GroupsDataType(GroupsEntry.cities, fields));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public GroupsQueryTest() {
        super(GroupsDataType.getManager());
    }
    
    @Test
    public void testSame() throws Exception {
        log.debug("------  testSame  ------");
        
        String chico = "'chiCO'";
        String chicoQuery = GroupField.CITY_EAST.getQueryField() + EQ_OP + chico;
        Collection<String> chicoResp = getExpectedKeyResponse(chicoQuery);
        
        String dover = "'doVer'";
        String doverQuery = GroupField.CITY_EAST.getQueryField() + EQ_OP + dover;
        Collection<String> doverResp = getExpectedKeyResponse(doverQuery);
        
        // mix/match responses - should be the same
        runTest(doverQuery, chicoResp);
        runTest(chicoQuery, doverResp);
    }
    
    @Test
    public void testEquivalent() throws Exception {
        log.debug("------  testEquivalent  ------");
        
        String state = "'north carolina'";
        String query = GroupField.STATE_EAST.getQueryField() + EQ_OP + state;
        Collection<String> expectedKeys = getExpectedKeyResponse(query);
        
        String cityNC = "'durham'";
        String cityOR = "'corvallis'";
        String cityQuery = GroupField.CITY_EAST.getQueryField() + EQ_OP + cityNC + OR_OP + GroupField.CITY_WEST.getQueryField() + EQ_OP + cityOR;
        
        runTest(query, expectedKeys);
        runTest(cityQuery, expectedKeys);
    }
    
    @Test
    public void testCompositeRange() throws Exception {
        log.debug("------  testCompositeRange  ------");
        
        String state = "'oregon'";
        String one = "'olympia'";
        String two = "'salem'";
        int min = 40;
        int max = 170;
        String query = GroupField.STATE_EAST.getQueryField() + EQ_OP + state + AND_OP + "(" + GroupField.CITY_EAST.getQueryField() + EQ_OP + one + OR_OP
                        + GroupField.CITY_EAST.getQueryField() + EQ_OP + two + ")" + AND_OP + "((_Bounded_ = true) && ("
                        + GroupField.COUNT_EAST.getQueryField() + GT_OP + min + AND_OP + GroupField.COUNT_EAST.getQueryField() + LT_OP + max + "))";
        runTest(query, query);
    }
    
    @Test
    public void testCompositeMultiRange() throws Exception {
        log.debug("------  testCompositeMultiRange  ------");
        
        String state = "'oregon'";
        int minOne = 22;
        int maxOne = 44;
        int minTwo = 125;
        int maxTwo = 170;
        String query = GroupField.STATE_EAST.getQueryField() + EQ_OP + state + AND_OP + "((_Bounded_ = true) && (" + GroupField.COUNT_EAST.getQueryField()
                        + GT_OP + minOne + AND_OP + GroupField.COUNT_EAST.getQueryField() + LT_OP + maxOne + "))" + OR_OP + "((_Bounded_ = true) && ("
                        + GroupField.COUNT_EAST.getQueryField() + GT_OP + minTwo + AND_OP + GroupField.COUNT_EAST.getQueryField() + LT_OP + maxTwo + "))";
        runTest(query, query);
    }
    
    @Test
    public void testCompositeMultiOr() throws Exception {
        log.debug("------  testCompositeMultiOr  ------");
        
        String state = "'oregon'";
        int one = 155;
        int two = 36;
        int three = 54;
        int four = 66;
        String query = GroupField.STATE_EAST.getQueryField() + EQ_OP + state + AND_OP + "(" + GroupField.COUNT_EAST.getQueryField() + EQ_OP + one + OR_OP
                        + GroupField.COUNT_EAST.getQueryField() + EQ_OP + two + OR_OP + GroupField.COUNT_EAST.getQueryField() + EQ_OP + three + OR_OP
                        + GroupField.COUNT_EAST.getQueryField() + EQ_OP + four + ")";
        runTest(query, query);
    }
    
    @Test
    public void testCompositeComplex() throws Exception {
        log.debug("------  testCompositeComplex  ------");
        
        String state = "'oregon'";
        String salem = "'salem'";
        int s1 = 41;
        int rmin = 40;
        int rmax = 70;
        int v1 = 47;
        int v2 = 36;
        int v3 = 155;
        String query = GroupField.STATE_EAST.getQueryField() + EQ_OP + state + AND_OP + "((" + GroupField.CITY_EAST.getQueryField() + EQ_OP + salem + AND_OP
                        + GroupField.COUNT_EAST.getQueryField() + EQ_OP + s1 + ")" + OR_OP + "((_Bounded_ = true) && (" + GroupField.COUNT_EAST.getQueryField()
                        + GT_OP + rmin + AND_OP + GroupField.COUNT_EAST.getQueryField() + LT_OP + rmax + "))" + OR_OP + "("
                        + GroupField.COUNT_EAST.getQueryField() + EQ_OP + v1 + OR_OP + GroupField.COUNT_EAST.getQueryField() + EQ_OP + v2 + OR_OP
                        + GroupField.COUNT_EAST.getQueryField() + EQ_OP + v3 + "))";
        runTest(query, query);
    }
    
    // end of unit tests
    // ============================================
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = GroupsDataType.getTestAuths();
        this.documentKey = GroupField.EVENT_ID.name();
    }
}
