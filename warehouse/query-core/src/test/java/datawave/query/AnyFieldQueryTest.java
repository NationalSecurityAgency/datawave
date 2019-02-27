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
import datawave.query.testframework.RawDataManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static datawave.query.testframework.RawDataManager.RN_OP;

public class AnyFieldQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(AnyFieldQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addReverseIndexField(CityField.STATE.name());
        generic.addReverseIndexField(CityField.CONTINENT.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public AnyFieldQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testEqual() throws Exception {
        log.info("------  testEqual  ------");
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String anyCity = this.dataManager.convertAnyField(cityPhrase);
            String query = Constants.ANY_FIELD + cityPhrase;
            runTest(query, anyCity);
        }
    }
    
    @Test
    public void testNotEqual() throws Exception {
        log.info("------  testNotEqual  ------");
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = " != " + "'" + city.name() + "'";
            String anyCity = this.dataManager.convertAnyField(cityPhrase, RawDataManager.AND_OP);
            String query = Constants.ANY_FIELD + cityPhrase;
            try {
                runTest(query, anyCity);
                Assert.fail("expecting exception");
            } catch (FullTableScansDisallowedException e) {
                // expected
            }
            
            this.logic.setFullTableScanEnabled(true);
            try {
                runTest(query, anyCity);
            } finally {
                this.logic.setFullTableScanEnabled(false);
            }
        }
    }
    
    @Test
    public void testAnd() throws Exception {
        log.info("------  testAnd  ------");
        String cont = "europe";
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String anyCity = this.dataManager.convertAnyField(cityPhrase);
            String contPhrase = EQ_OP + "'" + cont + "'";
            String anyCont = this.dataManager.convertAnyField(contPhrase);
            String query = Constants.ANY_FIELD + cityPhrase + AND_OP + Constants.ANY_FIELD + contPhrase;
            String anyQuery = anyCity + AND_OP + anyCont;
            runTest(query, anyQuery);
        }
    }
    
    @Test
    public void testOr() throws Exception {
        log.info("------  testOr  ------");
        String state = "mississippi";
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String anyCity = this.dataManager.convertAnyField(cityPhrase);
            String statePhrase = EQ_OP + "'" + state + "'";
            String anyState = this.dataManager.convertAnyField(statePhrase);
            String query = Constants.ANY_FIELD + cityPhrase + OR_OP + Constants.ANY_FIELD + statePhrase;
            String anyQuery = anyCity + OR_OP + anyState;
            runTest(query, anyQuery);
        }
    }
    
    @Test
    public void testOrOr() throws Exception {
        log.info("------  testOrOr  ------");
        String state = "mississippi";
        String cont = "none";
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String anyCity = this.dataManager.convertAnyField(cityPhrase);
            String statePhrase = EQ_OP + "'" + state + "'";
            String anyState = this.dataManager.convertAnyField(statePhrase);
            String contPhrase = EQ_OP + "'" + cont + "'";
            String anyCont = this.dataManager.convertAnyField(contPhrase);
            String query = "(" + Constants.ANY_FIELD + cityPhrase + OR_OP + Constants.ANY_FIELD + statePhrase + ")" + OR_OP + Constants.ANY_FIELD + contPhrase;
            String anyQuery = anyCity + OR_OP + anyState + OR_OP + anyCont;
            runTest(query, anyQuery);
        }
    }
    
    @Test
    public void testOrAnd() throws Exception {
        log.info("------  testOrAnd  ------");
        String state = "missISsippi";
        String cont = "EUrope";
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String anyCity = this.dataManager.convertAnyField(cityPhrase);
            String statePhrase = EQ_OP + "'" + state + "'";
            String anyState = this.dataManager.convertAnyField(statePhrase);
            String contPhrase = EQ_OP + "'" + cont + "'";
            String anyCont = this.dataManager.convertAnyField(contPhrase);
            String query = Constants.ANY_FIELD + cityPhrase + OR_OP + Constants.ANY_FIELD + statePhrase + AND_OP + Constants.ANY_FIELD + contPhrase;
            String anyQuery = anyCity + OR_OP + anyState + AND_OP + anyCont;
            runTest(query, anyQuery);
        }
    }
    
    @Test
    public void testAndAnd() throws Exception {
        log.info("------  testAndAnd  ------");
        String state = "'misSIssippi'";
        String cont = "'noRth amErIca'";
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String anyCity = this.dataManager.convertAnyField(cityPhrase);
            String statePhrase = EQ_OP + state;
            String anyState = this.dataManager.convertAnyField(statePhrase);
            String contPhrase = EQ_OP + cont;
            String anyCont = this.dataManager.convertAnyField(contPhrase);
            String query = Constants.ANY_FIELD + cityPhrase + AND_OP + Constants.ANY_FIELD + statePhrase + AND_OP + Constants.ANY_FIELD + contPhrase;
            String anyQuery = anyCity + AND_OP + anyState + AND_OP + anyCont;
            runTest(query, anyQuery);
        }
    }
    
    @Test
    public void testReverseIndex() throws Exception {
        log.info("------  testReverseIndex  ------");
        String phrase = EQ_OP + "'.*o'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testEqualNoMatch() throws Exception {
        log.info("------  testEqualNoMatch  ------");
        String phrase = EQ_OP + "'nothing'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testAndNoMatch() throws Exception {
        log.info("------  testAndNoMatch  ------");
        String phrase = EQ_OP + "'nothing'";
        String first = CityField.ACCESS.name() + EQ_OP + "'NA'";
        String query = first + AND_OP + Constants.ANY_FIELD + phrase;
        String expect = first + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testRegex() throws Exception {
        String phrase = RE_OP + "'ro.*'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testRegexZeroResults() throws Exception {
        String phrase = RE_OP + "'zero.*'";
        for (TestCities city : TestCities.values()) {
            String qCity = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'";
            String query = qCity + AND_OP + Constants.ANY_FIELD + phrase;
            String expect = qCity + AND_OP + this.dataManager.convertAnyField(phrase);
            runTest(query, expect);
        }
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testRegexWithFIAndRI() throws Exception {
        String phrase = RE_OP + "'.*iss.*'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(EQ_OP + "'nothing'");
        runTest(query, expect);
    }
    
    @Test
    public void testRegexOr() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String anyRo = this.dataManager.convertAnyField(roPhrase);
        String oPhrase = RE_OP + "'.*o'";
        String anyO = this.dataManager.convertAnyField(oPhrase);
        String query = Constants.ANY_FIELD + roPhrase + OR_OP + Constants.ANY_FIELD + oPhrase;
        String expect = anyRo + OR_OP + anyO;
        runTest(query, expect);
    }
    
    @Test
    public void testRegexAnd() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String anyRo = this.dataManager.convertAnyField(roPhrase);
        String oPhrase = RE_OP + "'.*o'";
        String anyO = this.dataManager.convertAnyField(oPhrase);
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + Constants.ANY_FIELD + oPhrase;
        String expect = anyRo + AND_OP + anyO;
        runTest(query, expect);
    }
    
    @Test
    public void testRegexAndField() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String anyRo = this.dataManager.convertAnyField(roPhrase);
        String oPhrase = RE_OP + "'.*o'";
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + CityField.STATE.name() + oPhrase;
        String expect = anyRo + AND_OP + CityField.STATE.name() + oPhrase;
        runTest(query, expect);
    }
    
    @Test
    public void testRegexAndFieldEqual() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String anyRo = this.dataManager.convertAnyField(roPhrase);
        String oPhrase = EQ_OP + "'ohio'";
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + CityField.STATE.name() + oPhrase;
        String expect = anyRo + AND_OP + CityField.STATE.name() + oPhrase;
        runTest(query, expect);
    }
    
    @Test
    public void testRegexReverseIndex() throws Exception {
        String regPhrase = RE_OP + "'.*ica'";
        String expect = this.dataManager.convertAnyField(regPhrase);
        String query = Constants.ANY_FIELD + regPhrase;
        runTest(query, expect);
    }
    
    @Test
    public void testNegRegex() throws Exception {
        String regPhrase = RN_OP + "'.*ica'";
        String expect = this.dataManager.convertAnyField(regPhrase, AND_OP);
        String query = Constants.ANY_FIELD + regPhrase;
        try {
            runTest(query, expect);
            Assert.fail("full table scan exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        try {
            this.logic.setFullTableScanEnabled(true);
            runTest(query, expect);
        } finally {
            this.logic.setFullTableScanEnabled(false);
        }
    }
    
    @Test
    public void testNegRegexAnd() throws Exception {
        String regPhrase = RN_OP + "'.*ica'";
        String negReg = this.dataManager.convertAnyField(regPhrase, AND_OP);
        try {
            this.logic.setFullTableScanEnabled(true);
            for (final TestCities city : TestCities.values()) {
                String cityPhrase = EQ_OP + "'" + city.name() + "'";
                String anyCity = this.dataManager.convertAnyField(cityPhrase);
                String query = Constants.ANY_FIELD + cityPhrase + AND_OP + Constants.ANY_FIELD + regPhrase;
                String expect = anyCity + AND_OP + negReg;
                runTest(query, expect);
            }
        } finally {
            this.logic.setFullTableScanEnabled(false);
        }
    }
    
    @Test
    public void testNegRegexOr() throws Exception {
        String regPhrase = RN_OP + "'.*ica'";
        String negReg = this.dataManager.convertAnyField(regPhrase, AND_OP);
        try {
            this.logic.setFullTableScanEnabled(true);
            for (final TestCities city : TestCities.values()) {
                String cityPhrase = EQ_OP + "'" + city.name() + "'";
                String anyCity = this.dataManager.convertAnyField(cityPhrase);
                String query = Constants.ANY_FIELD + cityPhrase + OR_OP + Constants.ANY_FIELD + regPhrase;
                String expect = anyCity + OR_OP + negReg;
                runTest(query, expect);
            }
        } finally {
            this.logic.setFullTableScanEnabled(false);
        }
    }
    
    // ============================================
    // private methods
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
