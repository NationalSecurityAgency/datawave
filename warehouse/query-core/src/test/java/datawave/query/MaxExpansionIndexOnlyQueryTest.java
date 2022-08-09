package datawave.query;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupExtension;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.MaxExpandCityFields;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.NOT_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static org.junit.jupiter.api.Assertions.fail;

public class MaxExpansionIndexOnlyQueryTest extends AbstractFunctionalQuery {
    
    @RegisterExtension
    public static AccumuloSetupExtension accumuloSetup = new AccumuloSetupExtension();
    
    private static final Logger log = Logger.getLogger(MaxExpansionRegexQueryTest.class);
    
    @BeforeAll
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig max = new MaxExpandCityFields();
        max.addIndexOnlyField(CitiesDataType.CityField.CITY.name());
        max.addIndexOnlyField(CitiesDataType.CityField.STATE.name());
        
        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.maxExp, max));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public MaxExpansionIndexOnlyQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    // ===================================
    // test cases
    
    @Test
    public void testMaxValueRegexIndexOnly() throws Exception {
        log.info("------  testMaxValueRegexIndexOnly  ------");
        // set regex to match multiple fields
        String city = EQ_OP + "'a-1'";
        String code = RE_OP + "'b.*'";
        
        String query = CitiesDataType.CityField.CITY.name() + city + AND_OP + CitiesDataType.CityField.STATE.name() + code;
        
        this.logic.setMaxValueExpansionThreshold(20);
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(2);
        try {
            runTest(query, query);
            fail("exception expected");
        } catch (DatawaveFatalQueryException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, query);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    @Test
    public void testMaxValueAnyField() throws Exception {
        log.info("------  testMaxValueAnyField  ------");
        String regexT = RE_OP + "'b-.*'";
        String regexA = RE_OP + "'a-.*'";
        String query = Constants.ANY_FIELD + regexT + AND_OP + Constants.ANY_FIELD + regexA;
        String anyT = this.dataManager.convertAnyField(regexT);
        String anyA = this.dataManager.convertAnyField(regexA);
        String expect = anyT + AND_OP + anyA;
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(2);
        try {
            runTest(query, expect);
            fail("exception expected");
        } catch (RuntimeException re) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
        
        this.logic.setMaxValueExpansionThreshold(1);
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 2);
    }
    
    @Test
    public void testMaxValueNegAnyField() throws Exception {
        log.info("------  testMaxValueNegAnyField  ------");
        String regexPhrase = RE_OP + "'a.*'";
        String country = "'b-StaTe'";
        String query = Constants.ANY_FIELD + EQ_OP + country + AND_OP + NOT_OP + "(" + Constants.ANY_FIELD + regexPhrase + ")";
        String expect = CitiesDataType.CityField.STATE.name() + EQ_OP + "'bi-s'";
        
        this.logic.setMaxValueExpansionThreshold(10);
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 0);
        
        this.logic.setMaxValueExpansionThreshold(1);
        try {
            runTest(query, expect);
            fail("exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        ivaratorConfig();
        runTest(query, expect);
        parsePlan(VALUE_THRESHOLD_JEXL_NODE, 1);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getExpansionAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
}
