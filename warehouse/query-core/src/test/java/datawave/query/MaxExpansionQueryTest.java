package datawave.query;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GTE_OP;
import static datawave.query.testframework.RawDataManager.LTE_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class MaxExpansionQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(MaxExpansionQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.COUNTRY.name());
        generic.addIndexField(CityField.CODE.name());
        generic.addIndexField(CityField.NUM.name());
        Set<String> idx = generic.getIndexFields();
        for (String field : idx) {
            generic.addReverseIndexField(field);
        }
        generic.addIndexOnlyField(CityField.COUNTRY.name());
        generic.addIndexOnlyField(CityField.CODE.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        FieldConfig paris = new GenericCityFields();
        paris.addIndexField(CityField.COUNTRY.name());
        paris.addIndexField(CityField.CODE.name());
        paris.addIndexField(CityField.NUM.name());
        idx = paris.getIndexFields();
        for (String field : idx) {
            paris.addReverseIndexField(field);
        }
        paris.addIndexOnlyField(CityField.COUNTRY.name());
        paris.addIndexOnlyField(CityField.CODE.name());
        dataTypes.add(new CitiesDataType(CityEntry.paris, paris));
        FieldConfig italy = new GenericCityFields();
        italy.addIndexField(CityField.COUNTRY.name());
        italy.addIndexField(CityField.CODE.name());
        italy.addIndexField(CityField.NUM.name());
        idx = italy.getIndexFields();
        for (String field : idx) {
            paris.addReverseIndexField(field);
        }
        italy.addIndexOnlyField(CityField.COUNTRY.name());
        italy.addIndexOnlyField(CityField.CODE.name());
        dataTypes.add(new CitiesDataType(CityEntry.italy, italy));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public MaxExpansionQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testMaxUnfielded() throws Exception {
        log.info("------  testMaxUnfielded  ------");
        
        // set regex to match multiple fields
        String regPhrase = RE_OP + "'.*e'";
        String expect = this.dataManager.convertAnyField(regPhrase);
        String query = Constants.ANY_FIELD + regPhrase;
        
        this.logic.setMaxUnfieldedExpansionThreshold(5);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxUnfieldedExpansionThreshold(1);
        try {
            runTest(query, expect);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
    }
    
    @Test
    public void testMaxValueOrState() throws Exception {
        log.info("------  testMaxValueOr  ------");
        String ohio = "'ohio'";
        String texas = "'texas'";
        String oregon = "'oregon'";
        String maine = "'maine'";
        // @formatter:off
        String query = CityField.STATE.name() + EQ_OP + ohio + OR_OP +
                CityField.STATE.name() + EQ_OP + texas + OR_OP +
                CityField.STATE.name() + EQ_OP + oregon + OR_OP +
                CityField.STATE.name() + EQ_OP + maine;
        // @formatter:on
        
        // query should work without OR thresholds
        runTest(query, query);
        
        this.logic.setCollapseUids(true);
        this.logic.setMaxOrExpansionThreshold(1);
        
        // ExceededOrThresholdMarkerJexlNode marker is added in PushdownLargeFieldedListsVistor when an ivarator is configured
        // the query does not change - to verify look at the log file for push down entries in log file
        ivaratorConfig();
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueOrCountry() throws Exception {
        log.info("------  testMaxValueOrCountry  ------");
        String spain = "'spain'";
        String france = "'france'";
        String usa = "'united states'";
        String italy = "'maine'";
        // @formatter:off
        String query = CityField.COUNTRY.name() + EQ_OP + spain + OR_OP +
                CityField.COUNTRY.name() + EQ_OP + france + OR_OP +
                CityField.COUNTRY.name() + EQ_OP + usa + OR_OP +
                CityField.COUNTRY.name() + EQ_OP + italy;
        // @formatter:on
        
        // query should work without OR thresholds
        runTest(query, query);
        
        // ExceededOrThresholdMarkerJexlNode marker is added in PushdownLargeFieldedListsVistor
        // to verify look at the log file for push down entries in log file
        this.logic.setCollapseUids(true);
        this.logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueOrFst() throws Exception {
        log.info("------  testMaxValueOrFst  ------");
        String spain = "'spain'";
        String france = "'france'";
        String usa = "'united states'";
        String italy = "'italy'";
        // @formatter:off
        String query = CityField.COUNTRY.name() + EQ_OP + spain + OR_OP +
                CityField.COUNTRY.name() + EQ_OP + france + OR_OP +
                CityField.COUNTRY.name() + EQ_OP + usa + OR_OP +
                CityField.COUNTRY.name() + EQ_OP + italy;
        // @formatter:on
        
        // query should work without OR thresholds
        runTest(query, query);
        
        // must have collapsible uids for pushdown to occur
        this.logic.setCollapseUids(true);
        this.logic.setMaxOrExpansionFstThreshold(1);
        ivaratorFstConfig();
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueOrFstNonIndexed() throws Exception {
        log.info("------  testMaxValueOrFstNonIndexed  ------");
        String spain = "'spain'";
        String france = "'france'";
        String usa = "'united states'";
        String italy = "'italy'";
        String paris = "'ParIs'";
        String venice = "'veNiCe'";
        String turin = "'TuriN'";
        // @formatter:off
        String query = "(" + CityField.CITY.name() + EQ_OP + paris + OR_OP +
                CityField.CITY.name() + EQ_OP + venice + OR_OP +
                CityField.CITY.name() + EQ_OP + turin + ")" + AND_OP +
                "(" + CityField.COUNTRY.name() + EQ_OP + spain + OR_OP +
                CityField.COUNTRY.name() + EQ_OP + france + OR_OP +
                CityField.COUNTRY.name() + EQ_OP + usa + OR_OP +
                CityField.COUNTRY.name() + EQ_OP + italy + ")";
        // @formatter:on
        // query should work without OR thresholds
        runTest(query, query);
        
        // must have collapsible uids for pushdown to occur
        this.logic.setCollapseUids(true);
        this.logic.setMaxOrExpansionFstThreshold(1);
        ivaratorFstConfig();
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueRangeOne() throws Exception {
        log.info("------  testMaxValueRangeOne  ------");
        String query = "((_Bounded_ = true) && (" + CityField.STATE.name() + LTE_OP + "'m~'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m'))";
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, query);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    @Test
    public void testMaxValueRangeTwo() throws Exception {
        log.info("------  testMaxValueRangeTwo  ------");
        String query = "((_Bounded_ = true) && (" + CityField.STATE.name() + LTE_OP + "'n'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m'))";
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, query);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    @Test
    public void testMaxValueRangeMutiHdfsLocations() throws Exception {
        log.info("------  testMaxValueRangeMultiHdfsLocations  ------");
        String query = "((_Bounded_ = true) && (" + CityField.STATE.name() + LTE_OP + "'n'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m'))";
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, query);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig(3, false);
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    @Test
    public void testMaxValueRangeIndexOnly() throws Exception {
        log.info("------  testMaxValueRangeIndexOnly  ------");
        // should match france and italy
        String cont = "'europe'";
        String query = CityField.CONTINENT.name() + EQ_OP + cont + AND_OP + "((_Bounded_ = true) && (" + CityField.COUNTRY.name() + " >= 'f' and "
                        + CityField.COUNTRY.name() + " <= 'j'))";
        
        this.logic.setMaxValueExpansionThreshold(3);
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, query);
            Assert.fail("exception expected");
        } catch (DatawaveFatalQueryException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    @Test
    public void testMaxValueFilter() throws Exception {
        log.info("------  testMaxValueFilter  ------");
        String query = "f:between(" + CityField.STATE.name() + ",'m','m~')";
        String expect = CityField.STATE.name() + LTE_OP + "'m~'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m'";
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, expect);
            Assert.fail("exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    @Test
    public void testMaxValueMultiFilter() throws Exception {
        log.info("------  testMaxValueMultiFilter  ------");
        String query = "f:between(" + CityField.STATE.name() + ",'m','m~') and f:between(" + CityField.STATE.name() + ",'mc','s')";
        String expect = "(" + CityField.STATE.name() + LTE_OP + "'m~'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m')" + " and (" + CityField.STATE.name()
                        + LTE_OP + "'s'" + AND_OP + CityField.STATE.name() + GTE_OP + "'mc')";
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(6);
        try {
            runTest(query, expect);
            Assert.fail("exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    @Test
    public void testNumericRange() throws Exception {
        String query = "((_Bounded_ = true) && (" + CityField.NUM.name() + GTE_OP + "99" + AND_OP + CityField.NUM.name() + LTE_OP + "131))";
        // should expand to EQNODES for 100, 110, 120, 130
        this.logic.setMaxValueExpansionThreshold(20);
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(3);
        try {
            runTest(query, query);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
