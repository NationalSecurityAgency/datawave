package datawave.query.planner;

import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.Constants;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.GenericCityFields;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static datawave.query.testframework.RawDataManager.JEXL_OR_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static datawave.query.testframework.RawDataManager.RN_OP;

public class AnyFieldQueryPlannerTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(AnyFieldQueryPlannerTest.class);
    
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
    
    public AnyFieldQueryPlannerTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testEqual() throws Exception {
        log.info("------  testEqual  ------");
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String anyCity = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyCity = "(" + anyCity + JEXL_OR_OP + CityField.STATE.name() + cityPhrase + ")";
            }
            String query = Constants.ANY_FIELD + cityPhrase;
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyCity, plan);
        }
    }
    
    @Test
    public void testNotEqual() throws Exception {
        log.info("------  testNotEqual  ------");
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = " != " + "'" + city.name() + "'";
            String anyCity = "(CITY == '" + city.name() + "'";
            if (city.name().equals("london")) {
                anyCity = '(' + anyCity + JEXL_OR_OP + "STATE == '" + city.name() + "'))";
            } else {
                anyCity = anyCity + ')';
            }
            anyCity = '!' + anyCity;
            String query = Constants.ANY_FIELD + cityPhrase;
            try {
                String plan = getPlan(query, true, true);
                Assert.fail("expecting exception");
            } catch (FullTableScansDisallowedException e) {
                // expected
            }
            
            this.logic.setFullTableScanEnabled(true);
            try {
                String plan = getPlan(query, true, true);
                assertPlanEquals(anyCity, plan);
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
            String contPhrase = EQ_OP + "'" + cont + "'";
            String query = Constants.ANY_FIELD + cityPhrase + AND_OP + Constants.ANY_FIELD + contPhrase;
            String anyQuery = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyQuery = "(" + anyQuery + JEXL_OR_OP + CityField.STATE.name() + cityPhrase + ")";
            }
            anyQuery += JEXL_AND_OP + CityField.CONTINENT.name() + contPhrase;
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyQuery, plan);
        }
    }
    
    @Test
    public void testOr() throws Exception {
        log.info("------  testOr  ------");
        String state = "mississippi";
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String statePhrase = EQ_OP + "'" + state + "'";
            String query = Constants.ANY_FIELD + cityPhrase + OR_OP + Constants.ANY_FIELD + statePhrase;
            String anyQuery = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyQuery = anyQuery + JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
            }
            anyQuery += JEXL_OR_OP + CityField.STATE.name() + statePhrase;
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyQuery, plan);
        }
    }
    
    @Test
    public void testOrOr() throws Exception {
        log.info("------  testOrOr  ------");
        String state = "mississippi";
        String cont = "none";
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String statePhrase = EQ_OP + "'" + state + "'";
            String contPhrase = EQ_OP + "'" + cont + "'";
            String query = "(" + Constants.ANY_FIELD + cityPhrase + OR_OP + Constants.ANY_FIELD + statePhrase + ")" + OR_OP + Constants.ANY_FIELD + contPhrase;
            String anyQuery = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyQuery = anyQuery + JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
            }
            anyQuery += JEXL_OR_OP + CityField.STATE.name() + statePhrase;
            anyQuery += JEXL_OR_OP + "_NOFIELD_" + contPhrase;
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyQuery, plan);
        }
    }
    
    @Test
    public void testOrAnd() throws Exception {
        log.info("------  testOrAnd  ------");
        String state = "missISsippi";
        String cont = "EUrope";
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String statePhrase = EQ_OP + "'" + state + "'";
            String contPhrase = EQ_OP + "'" + cont + "'";
            String query = Constants.ANY_FIELD + cityPhrase + OR_OP + Constants.ANY_FIELD + statePhrase + AND_OP + Constants.ANY_FIELD + contPhrase;
            String anyQuery = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyQuery = anyQuery + JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
            }
            anyQuery += JEXL_OR_OP + "(" + CityField.STATE.name() + statePhrase.toLowerCase();
            anyQuery += JEXL_AND_OP + CityField.CONTINENT.name() + contPhrase.toLowerCase() + ")";
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyQuery, plan);
        }
    }
    
    @Test
    public void testAndAnd() throws Exception {
        log.info("------  testAndAnd  ------");
        String state = "misSIssippi";
        String cont = "noRth amErIca";
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String statePhrase = EQ_OP + "'" + state + "'";
            String contPhrase = EQ_OP + "'" + cont + "'";
            String query = Constants.ANY_FIELD + cityPhrase + AND_OP + Constants.ANY_FIELD + statePhrase + AND_OP + Constants.ANY_FIELD + contPhrase;
            String anyQuery = CityField.CONTINENT.name() + contPhrase.toLowerCase() + JEXL_AND_OP;
            if (city.name().equals("london")) {
                anyQuery += "((" + CityField.STATE.name() + statePhrase.toLowerCase() + JEXL_AND_OP + CityField.STATE.name() + cityPhrase + ")" + JEXL_OR_OP;
            }
            anyQuery += CityField.CITY.name() + '_' + CityField.STATE.name() + EQ_OP + "'" + city.name() + CompositeIngest.DEFAULT_SEPARATOR
                            + state.toLowerCase() + "'";
            if (city.name().equals("london")) {
                anyQuery += ")";
            }
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyQuery, plan);
        }
    }
    
    @Test
    public void testReverseIndex() throws Exception {
        log.info("------  testReverseIndex  ------");
        String phrase = EQ_OP + "'.*o'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = "_NOFIELD_" + phrase;
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
    }
    
    @Test
    public void testEqualNoMatch() throws Exception {
        log.info("------  testEqualNoMatch  ------");
        String phrase = EQ_OP + "'nothing'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = "_NOFIELD_ == 'nothing'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
    }
    
    @Test
    public void testAndNoMatch() throws Exception {
        log.info("------  testAndNoMatch  ------");
        String phrase = EQ_OP + "'nothing'";
        String first = CityField.ACCESS.name() + EQ_OP + "'NA'";
        String query = first + AND_OP + Constants.ANY_FIELD + phrase;
        String expect = '(' + CityField.ACCESS.name() + EQ_OP + "'na'" + JEXL_OR_OP + first + ")" + JEXL_AND_OP + "_NOFIELD_ == 'nothing'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
    }
    
    @Test
    public void testRegex() throws Exception {
        String phrase = RE_OP + "'ro.*'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = CityField.CITY.name() + EQ_OP + "'rome'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
    }
    
    @Test
    public void testRegexZeroResults() throws Exception {
        String phrase = RE_OP + "'zero.*'";
        for (TestCities city : TestCities.values()) {
            String qCity = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'";
            String query = qCity + AND_OP + Constants.ANY_FIELD + phrase;
            String expect = qCity + JEXL_AND_OP + "((ASTDelayedPredicate = true)" + JEXL_AND_OP + "(_NOFIELD_" + phrase + "))";
            String plan = getPlan(query, true, true);
            assertPlanEquals(expect, plan);
        }
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testRegexWithFIAndRI() throws Exception {
        String phrase = RE_OP + "'.*iss.*'";
        String query = Constants.ANY_FIELD + phrase;
        String plan = getPlan(query, true, true);
    }
    
    @Test
    public void testRegexOr() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String oPhrase = RE_OP + "'.*o'";
        String query = Constants.ANY_FIELD + roPhrase + OR_OP + Constants.ANY_FIELD + oPhrase;
        String expect = CityField.CITY.name() + EQ_OP + "'rome'" + JEXL_OR_OP + CityField.STATE.name() + EQ_OP + "'lazio'" + JEXL_OR_OP
                        + CityField.STATE.name() + EQ_OP + "'ohio'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
    }
    
    @Test
    public void testRegexAnd() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String oPhrase = RE_OP + "'.*o'";
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + Constants.ANY_FIELD + oPhrase;
        String compositeField = CityField.CITY.name() + '_' + CityField.STATE.name();
        String expect = "(" + compositeField + EQ_OP + "'rome" + CompositeIngest.DEFAULT_SEPARATOR + "lazio'" + JEXL_OR_OP + compositeField + EQ_OP + "'rome"
                        + CompositeIngest.DEFAULT_SEPARATOR + "ohio')";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
    }
    
    @Test
    public void testRegexAndField() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String oPhrase = RE_OP + "'.*o'";
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + CityField.STATE.name() + oPhrase;
        String expect = "(" + CityField.CITY.name() + '_' + CityField.STATE.name() + EQ_OP + "'rome" + CompositeIngest.DEFAULT_SEPARATOR + "lazio'"
                        + JEXL_OR_OP + CityField.CITY.name() + '_' + CityField.STATE.name() + EQ_OP + "'rome" + CompositeIngest.DEFAULT_SEPARATOR + "ohio')"
                        + JEXL_AND_OP + "((ASTEvaluationOnly = true)" + JEXL_AND_OP + "(" + CityField.CITY.name() + " == 'rome'" + JEXL_AND_OP + "("
                        + CityField.STATE.name() + " == 'lazio'" + JEXL_OR_OP + CityField.STATE.name() + " == 'ohio')))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
    }
    
    @Test
    public void testRegexAndFieldEqual() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String oPhrase = EQ_OP + "'ohio'";
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + CityField.STATE.name() + oPhrase;
        String expect = CityField.CITY.name() + '_' + CityField.STATE.name() + EQ_OP + "'rome" + CompositeIngest.DEFAULT_SEPARATOR + "ohio'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
    }
    
    @Test
    public void testRegexReverseIndex() throws Exception {
        String regPhrase = RE_OP + "'.*ica'";
        String expect = CityField.CONTINENT.name() + EQ_OP + "'north america'";
        String query = Constants.ANY_FIELD + regPhrase;
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
    }
    
    @Test
    public void testNegRegex() throws Exception {
        String regPhrase = RN_OP + "'.*ica'";
        String expect = "!(" + CityField.CONTINENT.name() + EQ_OP + "'north america')";
        String query = Constants.ANY_FIELD + regPhrase;
        try {
            String plan = getPlan(query, true, true);
            Assert.fail("full table scan exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        }
        
        try {
            this.logic.setFullTableScanEnabled(true);
            String plan = getPlan(query, true, true);
            assertPlanEquals(expect, plan);
        } finally {
            this.logic.setFullTableScanEnabled(false);
        }
    }
    
    @Test
    public void testNegRegexAnd() throws Exception {
        String regPhrase = RN_OP + "'.*ica'";
        try {
            this.logic.setFullTableScanEnabled(true);
            for (final TestCities city : TestCities.values()) {
                String cityPhrase = EQ_OP + "'" + city.name() + "'";
                String query = Constants.ANY_FIELD + cityPhrase + AND_OP + Constants.ANY_FIELD + regPhrase;
                String expect = CityField.CITY.name() + cityPhrase;
                if (city.name().equals("london")) {
                    expect = "(" + expect + JEXL_OR_OP + CityField.STATE.name() + cityPhrase + ")";
                }
                expect += JEXL_AND_OP + "!(" + CityField.CONTINENT.name() + EQ_OP + "'north america')";
                String plan = getPlan(query, true, true);
                assertPlanEquals(expect, plan);
            }
        } finally {
            this.logic.setFullTableScanEnabled(false);
        }
    }
    
    @Test
    public void testNegRegexOr() throws Exception {
        String regPhrase = RN_OP + "'.*ica'";
        try {
            this.logic.setFullTableScanEnabled(true);
            for (final TestCities city : TestCities.values()) {
                String cityPhrase = EQ_OP + "'" + city.name() + "'";
                String query = Constants.ANY_FIELD + cityPhrase + OR_OP + Constants.ANY_FIELD + regPhrase;
                String expect = CityField.CITY.name() + cityPhrase;
                if (city.name().equals("london")) {
                    expect += JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
                }
                expect += JEXL_OR_OP + "!(" + CityField.CONTINENT + EQ_OP + "'north america')";
                String plan = getPlan(query, true, true);
                assertPlanEquals(expect, plan);
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
