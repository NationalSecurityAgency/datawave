package datawave.query;

import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.language.functions.jexl.EvaluationOnly;
import datawave.query.language.functions.jexl.JexlQueryFunction;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static org.junit.Assert.assertEquals;

public class LuceneQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(LuceneQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        FieldConfig generic = new GenericCityFields();
        accumuloSetup.setData(FileType.CSV, new CitiesDataType(CityEntry.generic, generic));
        connector = accumuloSetup.loadTables(log);
    }
    
    public LuceneQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testSimpleEq() throws Exception {
        log.info("------  testSimpleEq  ------");
        String city = "rome";
        String query = CityField.CITY.name() + ":\"" + city + "\"";
        
        String expect = CityField.CITY.name() + EQ_OP + "'" + city + "'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        runTest(query, expect);
    }
    
    @Test
    public void testSimpleAndEq() throws Exception {
        log.info("------  testSimpleAndEq  ------");
        String city = "rome";
        String state = "lazio";
        String query = CityField.CITY.name() + ":\"" + city + "\"" + AND_OP + CityField.STATE.name() + ":\"" + state + "\"";
        
        String expect = CityField.CITY.name() + '_' + CityField.STATE.name() + EQ_OP + "'" + city + CompositeIngest.DEFAULT_SEPARATOR + state + "'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        // The expected results generator can't handle composite queries, so we'll just run the non-composite form of the query to compare results
        String expectNonComposite = CityField.CITY.name() + EQ_OP + "\"" + city + "\"" + JEXL_AND_OP + CityField.STATE.name() + EQ_OP + "\"" + state + "\"";
        
        runTest(query, expectNonComposite);
    }
    
    @Test
    public void testSimpleAndEqEvalOnly() throws Exception {
        log.info("------  testSimpleAndEqEvalOnly  ------");
        String city = "rome";
        String country = "italy";
        String query = CityField.CITY.name() + ":\"" + city + "\"" + AND_OP + "#EVALUATION_ONLY('" + CityField.COUNTRY.name() + ":\"" + country + "\"')";
        
        String expect = CityField.CITY.name() + EQ_OP + "'" + city + "'" + AND_OP + "((_Eval_ = true) && " + CityField.COUNTRY.name() + EQ_OP + "'" + country
                        + "')";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        runTest(query, expect);
    }
    
    @Test
    public void testAnyFieldInclude() throws Exception {
        log.info("------  testAnyFieldInclude  ------");
        String code = "europe";
        String state = "lazio";
        String phrase = RE_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#INCLUDE(" + state + ")";
        
        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + JEXL_AND_OP + "filter:includeRegex(" + Constants.ANY_FIELD + ", '" + state
                        + "')";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testExplicitAnyFieldInclude() throws Exception {
        log.info("------  testExplicitAnyFieldInclude  ------");
        String code = "europe";
        String state = "lazio";
        String phrase = RE_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#INCLUDE(" + Constants.ANY_FIELD + "," + state + ")";
        
        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + JEXL_AND_OP + "(filter:includeRegex(" + Constants.ANY_FIELD + ", '" + state
                        + "'))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testAnyFieldWithRegex() throws Exception {
        log.info("------  testAnyField  ------");
        String code = "europe";
        String state = "l.*";
        String phrase = RE_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#INCLUDE(" + Constants.ANY_FIELD + "," + state + ")";
        
        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + JEXL_AND_OP + "(filter:includeRegex(" + Constants.ANY_FIELD + ", '" + state
                        + "'))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testExplicitFieldEvaluationOnlyWithRegex() throws Exception {
        log.info("------  testExplicitFieldRegexEvaluationOnly  ------");
        String code = "europe";
        String state = "l.*";
        String phrase = RE_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#JEXL(\"((_Eval_ = true)" + JEXL_AND_OP + CityField.STATE.name() + " =~ '"
                        + state + "')\")";
        
        String expect = CityField.CONTINENT.name() + " == '" + code + "'" + JEXL_AND_OP + "((_Eval_ = true)" + JEXL_AND_OP + CityField.STATE.name() + " =~ '"
                        + state + "')";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testExplicitFieldEvaluationOnlyWithRange() throws Exception {
        log.info("------  testExplicitFieldRange  ------");
        String code = "europe";
        String startState = "alabama";
        String endState = "wyoming";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#JEXL(\"((_Eval_ = true)" + JEXL_AND_OP + CityField.STATE.name() + " >= '"
                        + startState + "'" + JEXL_AND_OP + CityField.STATE.name() + " <= '" + endState + "')\")";
        
        String expect = CityField.CONTINENT.name() + " == '" + code + "'" + JEXL_AND_OP + "((_Eval_ = true)" + JEXL_AND_OP + "(" + CityField.STATE.name()
                        + " >= '" + startState + "'" + JEXL_AND_OP + CityField.STATE.name() + " <= '" + endState + "'))";
        String plan = getPlan(query, true, true);
        assertEquals(expect, plan);
        assertPlanEquals(expect, plan);
        
        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + CityField.STATE.name() + " >= '" + startState + "'" + AND_OP
                        + CityField.STATE.name() + " <= '" + endState + "'";
        runTest(query, expect);
    }
    
    @Test
    public void testMultiRangeSameField() throws Exception {
        log.info("------  testMultiRangeSameField  ------");
        logic.setMaxValueExpansionThreshold(1);
        logic.setFullTableScanEnabled(true);
        String code = "europe";
        String startState1 = "a";
        String endState1 = "lon";
        String startState2 = "hawaii";
        String endState2 = "wyoming";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + CityField.STATE.name() + ":[" + startState1 + " TO " + endState1 + "]"
                        + CityField.STATE.name() + ":[" + startState2 + " TO " + endState2 + "]";
        
        String expect = CityField.CONTINENT.name() + " == '" + code + "'" + JEXL_AND_OP + "((_Value_ = true) && ((_Bounded_ = true)" + JEXL_AND_OP + "("
                        + CityField.STATE.name() + " >= '" + startState1 + "'" + JEXL_AND_OP + CityField.STATE.name() + " <= '" + endState1 + "')))"
                        + JEXL_AND_OP + "((_Value_ = true) && ((_Bounded_ = true)" + JEXL_AND_OP + "(" + CityField.STATE.name() + " >= '" + startState2 + "'"
                        + JEXL_AND_OP + CityField.STATE.name() + " <= '" + endState2 + "')))";
        
        System.out.println("Expected: " + expect);
        String plan = getPlan(query, true, true);
        assertEquals(expect, plan);
        assertPlanEquals(expect, plan);
        
        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + CityField.STATE.name() + " >= '" + startState2 + "'" + AND_OP
                        + CityField.STATE.name() + " <= '" + endState1 + "'";
        runTest(query, expect);
    }
    
    @Test
    public void testExplicitFieldEvaluationOnlyWithRanges() throws Exception {
        log.info("------  testExplicitFieldRanges  ------");
        String code = "europe";
        String startState1 = "alabama";
        String endState1 = "connecticut";
        String startState2 = "iowa";
        String endState2 = "wyoming";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#JEXL(\"((_Eval_ = true)" + JEXL_AND_OP + CityField.STATE.name() + " >= '"
                        + startState1 + "'" + JEXL_AND_OP + CityField.STATE.name() + " <= '" + endState1 + "'" + JEXL_AND_OP + CityField.STATE.name() + " >= '"
                        + startState2 + "'" + JEXL_AND_OP + CityField.STATE.name() + " <= '" + endState2 + "'" + ")\")";
        
        String expect = CityField.CONTINENT.name() + " == '" + code + "'" + JEXL_AND_OP + "((_Eval_ = true)" + JEXL_AND_OP + "(" + CityField.STATE.name()
                        + " >= '" + startState1 + "'" + JEXL_AND_OP + CityField.STATE.name() + " <= '" + endState1 + "'" + JEXL_AND_OP + CityField.STATE.name()
                        + " >= '" + startState2 + "'" + JEXL_AND_OP + CityField.STATE.name() + " <= '" + endState2 + "'" + "))";
        String plan = getPlan(query, true, true);
        assertEquals(expect, plan);
        assertPlanEquals(expect, plan);
        
        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + CityField.STATE.name() + " >= '" + startState1 + "'" + AND_OP
                        + CityField.STATE.name() + " <= '" + endState1 + "'" + AND_OP + CityField.STATE.name() + " >= '" + startState2 + "'" + AND_OP
                        + CityField.STATE.name() + " <= '" + endState2 + "'";
        runTest(query, expect);
    }
    
    @Test
    public void testAnyFieldNotNullLiteral() throws Exception {
        log.info("------  testAnyField  ------");
        String cont = "europe";
        String state = "l.*";
        String phrase = RE_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + cont + "\"" + AND_OP + CityField.CITY.name() + ":*" + AND_OP + "#INCLUDE(" + Constants.ANY_FIELD
                        + "," + state + ")";
        
        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + cont + "'" + JEXL_AND_OP + "!(" + CityField.CITY.name() + EQ_OP + "null)" + JEXL_AND_OP
                        + "(filter:includeRegex(" + Constants.ANY_FIELD + ", '" + state + "'))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        expect = CityField.CONTINENT.name() + EQ_OP + "'" + cont + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testCompareFunction() throws Exception {
        log.info("------  testCompareFunction  ------");
        String lucene = "CITY:bar AND #COMPARE(CITY,<,ANY,STATE)";
        String expectedJexlPlan = "CITY == 'bar' && filter:compare(CITY,'<','ANY',STATE)";
        
        String jexlQueryPlan = getPlan(lucene, true, true);
        assertPlanEquals(expectedJexlPlan, jexlQueryPlan);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
        
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        
        for (JexlQueryFunction queryFunction : parser.getAllowedFunctions()) {
            if (queryFunction instanceof EvaluationOnly) {
                ((EvaluationOnly) queryFunction).setParser(parser);
                break;
            }
        }
        
        this.logic.setParser(parser);
    }
}
