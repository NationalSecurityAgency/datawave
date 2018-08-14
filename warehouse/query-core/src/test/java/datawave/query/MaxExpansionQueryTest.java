package datawave.query;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
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
        paris.addIndexOnlyField(CityField.COUNTRY.name());
        paris.addIndexOnlyField(CityField.CODE.name());
        dataTypes.add(new CitiesDataType(CityEntry.italy, italy));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
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
        
        this.logic.setMaxUnfieldedExpansionThreshold(1);
        try {
            runTest(query, expect);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
    }
    
    @Test
    public void testMaxValueRegexIndexOnly() throws Exception {
        log.info("------  testMaxValueRegexIndexOnly  ------");
        // set regex to match multiple fields
        String city = EQ_OP + "'rome'";
        String code = RE_OP + "'.*a'";
        String query = CityField.CITY.name() + city + AND_OP + CityField.CODE.name() + code;
        
        this.logic.setMaxValueExpansionThreshold(3);
        runTest(query, query);
        
        this.logic.setMaxValueExpansionThreshold(2);
        try {
            runTest(query, query);
            Assert.fail("exception expected");
        } catch (DatawaveFatalQueryException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueRegexAnyField() throws Exception {
        log.info("------  testMaxValueRegexAnyField  ------");
        // set regex to match multiple fields
        String regPhrase = RE_OP + "'.*e'";
        String expect = this.dataManager.convertAnyField(regPhrase);
        String query = Constants.ANY_FIELD + regPhrase;
        
        this.logic.setMaxValueExpansionThreshold(5);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        // set regex to match more fields than are specified for the unified expansion
        try {
            runTest(query, expect);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
    }
    
    @Test
    public void testMaxValueOr() throws Exception {
        log.info("------  testMaxValueOr  ------");
        String spain = "'spain'";
        String france = "'france'";
        String usa = "'united states'";
        String italy = "'maine'";
        String query = CityField.COUNTRY.name() + EQ_OP + spain + OR_OP + CityField.COUNTRY.name() + EQ_OP + france + OR_OP + CityField.COUNTRY.name() + EQ_OP
                        + usa + OR_OP + CityField.COUNTRY.name() + EQ_OP + italy;
        
        // query should work without OR thresholds
        runTest(query, query);
        
        this.logic.setCollapseUids(true);
        this.logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueOrFst() throws Exception {
        log.info("------  testMaxValueOr  ------");
        String spain = "'spain'";
        String france = "'france'";
        String usa = "'united states'";
        String italy = "'italy'";
        String query = CityField.COUNTRY.name() + EQ_OP + spain + OR_OP + CityField.COUNTRY.name() + EQ_OP + france + OR_OP + CityField.COUNTRY.name() + EQ_OP
                        + usa + OR_OP + CityField.COUNTRY.name() + EQ_OP + italy;
        
        // query should work without OR thresholds
        runTest(query, query);
        
        this.logic.setCollapseUids(true);
        this.logic.setMaxOrExpansionThreshold(1);
        this.logic.setMaxOrExpansionFstThreshold(1);
        ivaratorConfig();
        ivaratorFstConfig();
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueRangeOne() throws Exception {
        log.info("------  testMaxValueRangeOne  ------");
        String query = CityField.STATE.name() + LTE_OP + "'m~'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m'";
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, query);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, query);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueRangeTwo() throws Exception {
        log.info("------  testMaxValueRangeTwo  ------");
        String query = CityField.STATE.name() + LTE_OP + "'n'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m'";
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, query);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, query);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueRangeMutiHdfsLocations() throws Exception {
        log.info("------  testMaxValueRangeMultiHdfsLocations  ------");
        String query = CityField.STATE.name() + LTE_OP + "'n'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m'";
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, query);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, query);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig(3, false);
        this.logic.setMaxOrExpansionThreshold(1);
        this.logic.setMaxOrExpansionFstThreshold(1);
        runTest(query, query);
    }
    
    @Test
    public void testMaxValueRangeIndexOnly() throws Exception {
        log.info("------  testMaxValueRangeIndexOnly  ------");
        // should match france and italy
        String cont = "'europe'";
        String query = CityField.CONTINENT.name() + EQ_OP + cont + AND_OP + "(" + CityField.COUNTRY.name() + " >= 'f' and " + CityField.COUNTRY.name()
                        + " <= 'j')";
        
        this.logic.setMaxValueExpansionThreshold(3);
        runTest(query, query);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, query);
            Assert.fail("exception expected");
        } catch (DatawaveFatalQueryException e) {
            // expected
        }
    }
    
    @Test
    public void testMaxValueFilter() throws Exception {
        log.info("------  testMaxValueFilter  ------");
        String query = "f:between(" + CityField.STATE.name() + ",'m','m~')";
        String expect = CityField.STATE.name() + LTE_OP + "'m~'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m'";
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testMaxValueMultiFilter() throws Exception {
        log.info("------  testMaxValueMultiFilter  ------");
        String query = "f:between(" + CityField.STATE.name() + ",'m','m~') and f:between(" + CityField.STATE.name() + ",'mc','s')";
        String expect = "(" + CityField.STATE.name() + LTE_OP + "'m~'" + AND_OP + CityField.STATE.name() + GTE_OP + "'m')" + " and (" + CityField.STATE.name()
                        + LTE_OP + "'s'" + AND_OP + CityField.STATE.name() + GTE_OP + "'mc')";
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testMaxValueAnyFieldRegexExclude() throws Exception {
        log.info("------  testMaxValueAnyFieldRegexExclude  ------");
        String regexPhrase = RE_OP + "'m.*'";
        String exclude = "'Mississippi'";
        String query = Constants.ANY_FIELD + regexPhrase + " and filter:excludeRegex(" + CityField.STATE.name() + "," + exclude + ")";
        String anyState = this.dataManager.convertAnyField(regexPhrase);
        String expect = anyState + AND_OP + CityField.STATE.name() + " !~ " + exclude;
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testMaxValueAnyFieldOne() throws Exception {
        log.info("------  testMaxValueAnyFieldOne  ------");
        String regexPhrase = RE_OP + "'ma.*'";
        String state = "'Maine'";
        String query = Constants.ANY_FIELD + regexPhrase + AND_OP + Constants.ANY_FIELD + EQ_OP + state;
        String anyState = this.dataManager.convertAnyField(regexPhrase);
        String expect = anyState + AND_OP + CityField.STATE.name() + EQ_OP + state;
        
        this.logic.setMaxValueExpansionThreshold(5);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testMaxValueAnyFieldTwo() throws Exception {
        log.info("------  testMaxValueAnyFieldTwo  ------");
        String regexPhrase = RE_OP + "'m.*'";
        String state = "'Missouri'";
        String query = Constants.ANY_FIELD + regexPhrase + AND_OP + Constants.ANY_FIELD + EQ_OP + state;
        String anyState = this.dataManager.convertAnyField(regexPhrase);
        String expect = anyState + AND_OP + CityField.STATE.name() + EQ_OP + state;
        
        this.logic.setMaxValueExpansionThreshold(5);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testMaxValueAnyFieldNegRegex() throws Exception {
        log.info("------  testMaxValueAnyFieldNegRegex  ------");
        String regexPhrase = " !~ 'o.*'";
        String state = "'Missouri'";
        String query = Constants.ANY_FIELD + regexPhrase + AND_OP + Constants.ANY_FIELD + EQ_OP + state;
        String anyState = this.dataManager.convertAnyField(regexPhrase, AND_OP);
        String expect = anyState + AND_OP + CityField.STATE.name() + EQ_OP + state;
        
        this.logic.setMaxValueExpansionThreshold(5);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testMaxValueAnyFieldIndexOnly() throws Exception {
        log.info("------  testMaxValueAnyFieldIndexOnly  ------");
        String regexPhrase = RE_OP + "'p.*'";
        String country = "'FrancE'";
        String query = Constants.ANY_FIELD + regexPhrase + AND_OP + Constants.ANY_FIELD + EQ_OP + country;
        String anyState = this.dataManager.convertAnyField(regexPhrase);
        String expect = anyState + AND_OP + CityField.COUNTRY.name() + EQ_OP + country;
        
        this.logic.setQueryThreads(1);
        this.logic.setMaxValueExpansionThreshold(5);
        runTest(query, expect);
        
        // this.logic.setMaxValueExpansionThreshold(1);
        // ivaratorConfig();
        // runTest(query, expect);
    }
    
    @Test
    public void testMaxValueNegAnyFieldIndexOnlyOne() throws Exception {
        log.info("------  testMaxValueNegAnyFieldIndexOnlyOne  ------");
        String regexPhrase = RE_OP + "'unit.*'";
        String country = "'FrancE'";
        String query = Constants.ANY_FIELD + EQ_OP + country + AND_OP + "not(" + Constants.ANY_FIELD + regexPhrase + ")";
        String anyState = this.dataManager.convertAnyField(regexPhrase);
        String expect = CityField.COUNTRY.name() + EQ_OP + country + AND_OP + "not(" + anyState + ")";
        
        this.logic.setMaxValueExpansionThreshold(5);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testMaxValueNegAnyFieldIndexOnlyTwo() throws Exception {
        log.info("------  testMaxValueNegAnyFieldIndexOnlyTwo  ------");
        String regexPhrase = RE_OP + "'u.*'";
        String country = "'FrancE'";
        String query = Constants.ANY_FIELD + EQ_OP + country + AND_OP + "not(" + Constants.ANY_FIELD + regexPhrase + ")";
        String anyState = this.dataManager.convertAnyField(regexPhrase);
        String expect = CityField.COUNTRY.name() + EQ_OP + country + AND_OP + "not(" + anyState + ")";
        
        this.logic.setMaxValueExpansionThreshold(5);
        runTest(query, expect);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
    }
    
    @Test
    public void testNumericRange() throws Exception {
        String city = TestCities.rome.name();
        String query = "(" + CityField.NUM.name() + GTE_OP + "99" + AND_OP + CityField.NUM.name() + LTE_OP + "131)";
        // should expand to EQNODES for 100, 110, 120, 130
        this.logic.setMaxValueExpansionThreshold(4);
        runTest(query, query);
        
        this.logic.setMaxValueExpansionThreshold(3);
        try {
            runTest(query, query);
            Assert.fail("exception condition expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, query);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
