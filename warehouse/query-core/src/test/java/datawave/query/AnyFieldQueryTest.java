package datawave.query;

import com.google.common.collect.Multimap;
import datawave.data.ColumnFamilyConstants;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.rules.RegexPushdownTransformRule;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.query.testframework.RawDataManager;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static datawave.query.testframework.RawDataManager.JEXL_OR_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;
import static datawave.query.testframework.RawDataManager.RN_OP;
import static org.junit.Assert.fail;

public class AnyFieldQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(AnyFieldQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        FieldConfig generic = new GenericCityFields();
        generic.addReverseIndexField(CityField.STATE.name());
        generic.addReverseIndexField(CityField.CONTINENT.name());
        DataTypeHadoopConfig dataType = new CitiesDataType(CityEntry.generic, generic);
        
        accumuloSetup.setData(FileType.CSV, dataType);
        connector = accumuloSetup.loadTables(log);
    }
    
    public AnyFieldQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testEqual() throws Exception {
        log.info("------  testEqual  ------");
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String query = Constants.ANY_FIELD + cityPhrase;
            
            // Test the plan with all expansions
            String anyCity = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyCity = "(" + anyCity + JEXL_OR_OP + CityField.STATE.name() + cityPhrase + ")";
            }
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyCity, plan);
            
            // Test the plan sans value expansion
            plan = getPlan(query, true, false);
            assertPlanEquals(anyCity, plan);
            
            // Test the plan sans field expansion
            anyCity = Constants.ANY_FIELD + cityPhrase;
            plan = getPlan(query, false, true);
            assertPlanEquals(anyCity, plan);
            
            // test running the query
            anyCity = this.dataManager.convertAnyField(cityPhrase);
            runTest(query, anyCity);
        }
    }
    
    @Test
    public void testEqualMissesRemovedIndexedField() throws Exception {
        log.info("------  testEqualMissesRemovedIndexedField  ------");
        
        // The idea here is that now if we remove the indexed marker in the
        // metadata, then the anyfield expansion will miss the hit despite
        // the entry in the global index
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = EQ_OP + "'" + city.name() + "'";
            String query = Constants.ANY_FIELD + cityPhrase;
            
            // test running the query
            String anyCity = this.dataManager.convertAnyField(cityPhrase);
            runTest(query, anyCity);
            
            // remove the metadata entries
            Multimap<String,Key> metadata = removeMetadataEntries(JexlASTHelper.getIdentifierNames(JexlASTHelper.parseJexlQuery(anyCity)),
                            ColumnFamilyConstants.COLF_I);
            
            // expect no results
            runTest(query, Collections.emptyList());
            
            // add the metadata back in
            addMetadataEntries(metadata);
        }
    }
    
    @Test
    public void testNotEqual() throws Exception {
        log.info("------  testNotEqual  ------");
        for (final TestCities city : TestCities.values()) {
            String cityPhrase = " != " + "'" + city.name() + "'";
            String query = Constants.ANY_FIELD + cityPhrase;
            
            // Test the plan with all expansions
            try {
                String plan = getPlan(query, true, true);
                fail("Expected FullTableScanDisallowedException but got plan: " + plan);
            } catch (FullTableScansDisallowedException e) {
                // expected
            } catch (Exception e) {
                fail("Expected FullTableScanDisallowedException but got " + e);
            }
            
            // Test the plan sans value expansion
            String anyCity = "!(CITY == '" + city.name() + "'";
            if (city.name().equals("london")) {
                anyCity += ")" + JEXL_AND_OP + "!(STATE == '" + city.name() + "')";
            } else {
                anyCity = anyCity + ')';
            }
            anyCity = "(((!(" + Constants.ANY_FIELD + EQ_OP + "'" + city.name() + "')" + JEXL_AND_OP + anyCity + ")))";
            String plan = getPlan(query, true, false);
            assertPlanEquals(anyCity, plan);
            
            // Test the plan sans field expansion
            anyCity = "!(" + Constants.ANY_FIELD + EQ_OP + "'" + city.name() + "')";
            plan = getPlan(query, false, true);
            assertPlanEquals(anyCity, plan);
            
            // test running the query
            anyCity = this.dataManager.convertAnyField(cityPhrase, RawDataManager.AND_OP);
            try {
                runTest(query, anyCity);
                fail("expecting exception");
            } catch (FullTableScansDisallowedException e) {
                // expected
            } catch (Exception e) {
                fail("Expected FullTableScanDisallowedException but got " + e);
            }
            
            this.logic.setFullTableScanEnabled(true);
            try {
                // Test the plan with all expansions
                anyCity = "!(CITY == '" + city.name() + "'";
                if (city.name().equals("london")) {
                    anyCity += ")" + JEXL_AND_OP + "!(STATE == '" + city.name() + "')";
                } else {
                    anyCity = anyCity + ')';
                }
                anyCity = "(((!(" + Constants.ANY_FIELD + EQ_OP + "'" + city.name() + "') && " + anyCity + ")))";
                plan = getPlan(query, true, true);
                assertPlanEquals(anyCity, plan);
                
                // Test the plan sans value expansion
                plan = getPlan(query, true, false);
                assertPlanEquals(anyCity, plan);
                
                // Test the plan sans field expansion
                anyCity = "!(" + Constants.ANY_FIELD + EQ_OP + "'" + city.name() + "')";
                plan = getPlan(query, false, true);
                assertPlanEquals(anyCity, plan);
                
                anyCity = this.dataManager.convertAnyField(cityPhrase, RawDataManager.AND_OP);
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
            
            // Test the plan with all expansions
            String anyQuery = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyQuery = "(" + anyQuery + JEXL_OR_OP + CityField.STATE.name() + cityPhrase + ")";
            }
            anyQuery += JEXL_AND_OP + CityField.CONTINENT.name() + contPhrase;
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyQuery, plan);
            
            // Test the plan sans value expansion
            plan = getPlan(query, true, false);
            assertPlanEquals(anyQuery, plan);
            
            // Test the plan sans field expansion
            anyQuery = Constants.ANY_FIELD + cityPhrase + JEXL_AND_OP + Constants.ANY_FIELD + contPhrase;
            plan = getPlan(query, false, true);
            assertPlanEquals(anyQuery, plan);
            
            // test running the query
            anyQuery = anyCity + AND_OP + anyCont;
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
            
            // Test the plan with all expansions
            String anyQuery = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyQuery = anyQuery + JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
            }
            anyQuery += JEXL_OR_OP + CityField.STATE.name() + statePhrase;
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyQuery, plan);
            
            // Test the plan sans value expansion
            plan = getPlan(query, true, false);
            assertPlanEquals(anyQuery, plan);
            
            // Test the plan sans field expansion
            anyQuery = Constants.ANY_FIELD + cityPhrase + JEXL_OR_OP + Constants.ANY_FIELD + statePhrase;
            plan = getPlan(query, false, true);
            assertPlanEquals(anyQuery, plan);
            
            // test running the query
            anyQuery = anyCity + OR_OP + anyState;
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
            
            // Test the plan with all expansions
            String anyQuery = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyQuery = anyQuery + JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
            }
            anyQuery += JEXL_OR_OP + CityField.STATE.name() + statePhrase;
            String plan = getPlan(query, true, true);
            String expected = anyQuery.replace(JEXL_OR_OP + "_NOFIELD_" + contPhrase, "");
            assertPlanEquals(expected, plan);
            
            // Test the plan sans value expansion
            plan = getPlan(query, true, false);
            assertPlanEquals(expected, plan);
            
            // Test the plan sans field expansion
            anyQuery = Constants.ANY_FIELD + cityPhrase + JEXL_OR_OP + Constants.ANY_FIELD + statePhrase;
            plan = getPlan(query, false, true);
            assertPlanEquals(anyQuery, plan);
            
            // test running the query
            anyQuery = anyCity + OR_OP + anyState + OR_OP + anyCont;
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
            
            // Test the plan with all expansions
            String anyQuery = CityField.CITY.name() + cityPhrase;
            if (city.name().equals("london")) {
                anyQuery = anyQuery + JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
            }
            anyQuery += JEXL_OR_OP + "(" + CityField.STATE.name() + statePhrase.toLowerCase();
            anyQuery += JEXL_AND_OP + CityField.CONTINENT.name() + contPhrase.toLowerCase() + ")";
            String plan = getPlan(query, true, true);
            assertPlanEquals(anyQuery, plan);
            
            // Test the plan sans value expansion
            plan = getPlan(query, true, false);
            assertPlanEquals(anyQuery, plan);
            
            // Test the plan sans field expansion
            anyQuery = Constants.ANY_FIELD + cityPhrase.toLowerCase() + JEXL_OR_OP + "(" + Constants.ANY_FIELD + statePhrase.toLowerCase() + JEXL_AND_OP
                            + Constants.ANY_FIELD + contPhrase.toLowerCase() + ")";
            plan = getPlan(query, false, true);
            assertPlanEquals(anyQuery, plan);
            
            // test running the query
            anyQuery = anyCity + OR_OP + anyState + AND_OP + anyCont;
            runTest(query, anyQuery);
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
            
            // Test the plan with all expansions
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
            
            // Test the plan sans value expansion
            plan = getPlan(query, true, false);
            assertPlanEquals(anyQuery, plan);
            
            // Test the plan sans field expansion
            anyQuery = Constants.ANY_FIELD + cityPhrase + JEXL_AND_OP + Constants.ANY_FIELD + statePhrase.toLowerCase() + JEXL_AND_OP + Constants.ANY_FIELD
                            + contPhrase.toLowerCase();
            plan = getPlan(query, false, true);
            assertPlanEquals(anyQuery, plan);
            
            // test running the query
            String anyCity = this.dataManager.convertAnyField(cityPhrase);
            String anyState = this.dataManager.convertAnyField(statePhrase);
            String anyCont = this.dataManager.convertAnyField(contPhrase);
            anyQuery = anyCity + AND_OP + anyState + AND_OP + anyCont;
            runTest(query, anyQuery);
        }
    }
    
    @Test
    public void testReverseIndex() throws Exception {
        log.info("------  testReverseIndex  ------");
        String phrase = RE_OP + "'.*ica'";
        String query = Constants.ANY_FIELD + phrase;
        
        // Test the plan with all expansions
        String expect = CityField.CONTINENT.name() + EQ_OP + "'north america'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans value expansion
        expect = CityField.CONTINENT.name() + phrase;
        plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans field expansion
        expect = Constants.ANY_FIELD + EQ_OP + "'north america'";
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testReverseIndexMissesRemovedIndexEntries() throws Exception {
        log.info("------  testReverseIndexMissesRemovedIndexEntries  ------");
        // The idea here is that now if we remove the indexed marker in the
        // metadata, then the anyfield expansion will miss the hit despite
        // the entry in the global index
        String phrase = RE_OP + "'.*ica'";
        String query = Constants.ANY_FIELD + phrase;
        
        // Test the plan with all expansions
        // test running the query
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
        
        // remove the metadata entries
        Multimap<String,Key> metadata = removeMetadataEntries(JexlASTHelper.getIdentifierNames(JexlASTHelper.parseJexlQuery(expect)),
                        ColumnFamilyConstants.COLF_RI);
        
        // expect no results
        try {
            runTest(query, Collections.emptyList());
        } catch (FullTableScansDisallowedException e) {
            // ok, essential no matches in index
        }
        
        // add the metadata back in
        addMetadataEntries(metadata);
    }
    
    @Test
    public void testEqualNoMatch() throws Exception {
        log.info("------  testEqualNoMatch  ------");
        String phrase = EQ_OP + "'nothing'";
        String query = Constants.ANY_FIELD + phrase;
        
        // Test the plan with all expansions
        String expect = Constants.NO_FIELD + phrase;
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans value expansion
        plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans field expansion
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testAndNoMatch() throws Exception {
        log.info("------  testAndNoMatch  ------");
        String phrase = EQ_OP + "'nothing'";
        String first = CityField.ACCESS.name() + EQ_OP + "'NA'";
        String query = first + AND_OP + Constants.ANY_FIELD + phrase;
        
        // Test the plan with all expansions
        String expect = "false";
        String plan = getPlan(query, true, true);
        assertPlanEquals("false", plan);
        
        // Test the plan sans value expansion
        plan = getPlan(query, true, false);
        assertPlanEquals("false", plan);
        
        // Test the plan sans field expansion
        expect = "false";
        plan = getPlan(query, false, true);
        assertPlanEquals("false", plan);
        
        // test running the query
        expect = first + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testRegex() throws Exception {
        String phrase = RE_OP + "'ro.*'";
        String query = Constants.ANY_FIELD + phrase;
        
        // Test the plan with all expansions
        String expect = CityField.CITY.name() + EQ_OP + "'rome'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans value expansion
        expect = CityField.CITY.name() + phrase;
        plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans field expansion
        expect = Constants.ANY_FIELD + EQ_OP + "'rome'";
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testRegexMissesRemovedIndexEntries() throws Exception {
        // The idea here is that now if we remove the indexed marker in the
        // metadata, then the anyfield expansion will miss the hit despite
        // the entry in the global index
        String phrase = RE_OP + "'ro.*'";
        String query = Constants.ANY_FIELD + phrase;
        
        // test running the query
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
        
        // remove the metadata entries
        Multimap<String,Key> metadata = removeMetadataEntries(JexlASTHelper.getIdentifierNames(JexlASTHelper.parseJexlQuery(expect)),
                        ColumnFamilyConstants.COLF_I);
        
        // expect no results (or error until #567 is fixed)
        try {
            runTest(query, Collections.emptyList());
        } catch (FullTableScansDisallowedException e) {
            // ok
        }
        
        // add the metadata back in
        addMetadataEntries(metadata);
    }
    
    @Test
    public void testRegexZeroResults() throws Exception {
        String phrase = RE_OP + "'zero.*'";
        for (TestCities city : TestCities.values()) {
            String qCity = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'";
            String query = qCity + AND_OP + Constants.ANY_FIELD + phrase;
            
            // Test the plan with all expansions
            String expect = "false";
            String plan = getPlan(query, true, true);
            assertPlanEquals("false", plan);
            
            // Test the plan sans value expansion
            plan = getPlan(query, true, false);
            assertPlanEquals("false", plan);
            
            // Test the plan sans field expansion
            plan = getPlan(query, false, true);
            assertPlanEquals("false", plan);
            
            // test running the query
            expect = qCity + AND_OP + this.dataManager.convertAnyField(phrase);
            runTest(query, expect);
        }
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testRegexWithFIAndRI() throws Exception {
        String phrase = RE_OP + "'.*iss.*'";
        String query = Constants.ANY_FIELD + phrase;
        
        // Test the plan with all expansions
        try {
            String plan = getPlan(query, true, true);
            fail("Expected DatawaveFatalQueryException but got plan: " + plan);
        } catch (DatawaveFatalQueryException e) {
            // expected
        } catch (Exception e) {
            fail("Expected DatawaveFatalQueryException but got " + e);
        }
        
        // Test the plan sans value expansion
        try {
            String plan = getPlan(query, true, false);
            fail("Expected DatawaveFatalQueryException but got plan: " + plan);
        } catch (DatawaveFatalQueryException e) {
            // expected
        } catch (Exception e) {
            fail("Expected DatawaveFatalQueryException but got " + e);
        }
        
        // Test the plan sans field expansion
        try {
            String plan = getPlan(query, false, true);
            fail("Expected DatawaveFatalQueryException but got plan: " + plan);
        } catch (DatawaveFatalQueryException e) {
            // expected
        } catch (Exception e) {
            fail("Expected DatawaveFatalQueryException but got " + e);
        }
        
        // test running the query
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
        
        // Test the plan with all expansions
        String expect = CityField.CITY.name() + EQ_OP + "'rome'" + JEXL_OR_OP + CityField.STATE.name() + EQ_OP + "'lazio'" + JEXL_OR_OP
                        + CityField.STATE.name() + EQ_OP + "'ohio'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans value expansion
        expect = CityField.CITY.name() + roPhrase + JEXL_OR_OP + CityField.STATE.name() + oPhrase;
        plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans field expansion
        expect = Constants.ANY_FIELD + EQ_OP + "'rome'" + JEXL_OR_OP + Constants.ANY_FIELD + EQ_OP + "'lazio'" + JEXL_OR_OP + Constants.ANY_FIELD + EQ_OP
                        + "'ohio'";
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = anyRo + OR_OP + anyO;
        runTest(query, expect);
    }
    
    @Test
    public void testRegexAnd() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String anyRo = this.dataManager.convertAnyField(roPhrase);
        String oPhrase = RE_OP + "'.*o'";
        String anyO = this.dataManager.convertAnyField(oPhrase);
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + Constants.ANY_FIELD + oPhrase;
        
        // Test the plan with all expansions
        String compositeField = CityField.CITY.name() + '_' + CityField.STATE.name();
        String expect = "(" + compositeField + EQ_OP + "'rome" + CompositeIngest.DEFAULT_SEPARATOR + "lazio'" + JEXL_OR_OP + compositeField + EQ_OP + "'rome"
                        + CompositeIngest.DEFAULT_SEPARATOR + "ohio')";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans value expansion
        expect = CityField.CITY.name() + roPhrase + JEXL_AND_OP + CityField.STATE.name() + oPhrase;
        plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans field expansion
        expect = Constants.ANY_FIELD + EQ_OP + "'rome'" + JEXL_AND_OP + "(" + Constants.ANY_FIELD + EQ_OP + "'lazio'" + JEXL_OR_OP + Constants.ANY_FIELD
                        + EQ_OP + "'ohio')";
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = anyRo + AND_OP + anyO;
        runTest(query, expect);
    }
    
    @Test
    public void testRegexAndField() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String anyRo = this.dataManager.convertAnyField(roPhrase);
        String oPhrase = RE_OP + "'.*o'";
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + CityField.STATE.name() + oPhrase;
        
        // Test the plan with all expansions
        String expect = "(" + CityField.CITY.name() + '_' + CityField.STATE.name() + EQ_OP + "'rome" + CompositeIngest.DEFAULT_SEPARATOR + "lazio'"
                        + JEXL_OR_OP + CityField.CITY.name() + '_' + CityField.STATE.name() + EQ_OP + "'rome" + CompositeIngest.DEFAULT_SEPARATOR + "ohio')"
                        + JEXL_AND_OP + "((_Eval_ = true)" + JEXL_AND_OP + "(" + CityField.CITY.name() + " == 'rome'" + JEXL_AND_OP + CityField.STATE.name()
                        + oPhrase + "))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans value expansion
        expect = CityField.CITY.name() + roPhrase + JEXL_AND_OP + CityField.STATE.name() + oPhrase;
        plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans field expansion
        expect = Constants.ANY_FIELD + EQ_OP + "'rome'" + JEXL_AND_OP + "(" + CityField.STATE.name() + EQ_OP + "'lazio'" + JEXL_OR_OP + CityField.STATE.name()
                        + EQ_OP + "'ohio')";
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = anyRo + AND_OP + CityField.STATE.name() + oPhrase;
        runTest(query, expect);
    }
    
    @Test
    public void testRegexAndFieldEqual() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String anyRo = this.dataManager.convertAnyField(roPhrase);
        String oPhrase = EQ_OP + "'ohio'";
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + CityField.STATE.name() + oPhrase;
        
        // Test the plan with all expansions
        String expect = CityField.CITY.name() + '_' + CityField.STATE.name() + EQ_OP + "'rome" + CompositeIngest.DEFAULT_SEPARATOR + "ohio'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans value expansion
        expect = CityField.CITY.name() + roPhrase + JEXL_AND_OP + CityField.STATE.name() + oPhrase;
        plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans field expansion
        expect = Constants.ANY_FIELD + EQ_OP + "'rome'" + JEXL_AND_OP + CityField.STATE.name() + oPhrase;
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = anyRo + AND_OP + CityField.STATE.name() + oPhrase;
        runTest(query, expect);
    }
    
    @Test
    public void testRegexReverseIndex() throws Exception {
        String regPhrase = RE_OP + "'.*ica'";
        String query = Constants.ANY_FIELD + regPhrase;
        
        // Test the plan with all expansions
        String expect = CityField.CONTINENT.name() + EQ_OP + "'north america'";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans value expansion
        expect = CityField.CONTINENT.name() + regPhrase;
        plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans field expansion
        expect = Constants.ANY_FIELD + EQ_OP + "'north america'";
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = this.dataManager.convertAnyField(regPhrase);
        runTest(query, expect);
    }
    
    @Test
    public void testNegRegex() throws Exception {
        String regPhrase = RN_OP + "'.*ica'";
        String query = Constants.ANY_FIELD + regPhrase;
        
        // Test the plan with all expansions
        try {
            String plan = getPlan(query, true, true);
            fail("Expected FullTableScanDisallowedException but got plan: " + plan);
        } catch (FullTableScansDisallowedException e) {
            // expected
        } catch (Exception e) {
            fail("Expected FullTableScanDisallowedException but got " + e);
        }
        
        // Test the plan sans value expansion
        String expect = "(((!(((_Delayed_ = true)" + JEXL_AND_OP + "(" + Constants.ANY_FIELD + RE_OP + "'.*ica')))" + JEXL_AND_OP + "!("
                        + CityField.CONTINENT.name() + RE_OP + "'.*ica'))))";
        String plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // Test the plan sans field expansion
        expect = "(((!(((_Delayed_ = true)" + JEXL_AND_OP + "(" + Constants.ANY_FIELD + RE_OP + "'.*ica')))" + JEXL_AND_OP + "!(" + Constants.ANY_FIELD + EQ_OP
                        + "'north america'))))";
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = this.dataManager.convertAnyField(regPhrase, AND_OP);
        try {
            runTest(query, expect);
            fail("full table scan exception expected");
        } catch (FullTableScansDisallowedException e) {
            // expected
        } catch (Exception e) {
            fail("Expected FullTableScanDisallowedException but got " + e);
        }
        
        try {
            this.logic.setFullTableScanEnabled(true);
            
            // Test the plan with all expansions
            expect = "(((!(" + Constants.ANY_FIELD + RE_OP + "'.*ica')" + JEXL_AND_OP + "!(" + CityField.CONTINENT.name() + EQ_OP + "'north america'))))";
            plan = getPlan(query, true, true);
            assertPlanEquals(expect, plan);
            
            // Test the plan sans value expansion
            expect = "(((!(" + Constants.ANY_FIELD + RE_OP + "'.*ica')" + JEXL_AND_OP + "!(" + CityField.CONTINENT.name() + RE_OP + "'.*ica'))))";
            plan = getPlan(query, true, false);
            assertPlanEquals(expect, plan);
            
            // Test the plan sans field expansion
            expect = "(((!(" + Constants.ANY_FIELD + RE_OP + "'.*ica')" + JEXL_AND_OP + "!(" + Constants.ANY_FIELD + EQ_OP + "'north america'))))";
            plan = getPlan(query, false, true);
            assertPlanEquals(expect, plan);
            
            // test running the query
            expect = this.dataManager.convertAnyField(regPhrase, AND_OP);
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
                
                // Test the plan with all expansions
                String expect = CityField.CITY.name() + cityPhrase;
                if (city.name().equals("london")) {
                    expect = "(" + expect + JEXL_OR_OP + CityField.STATE.name() + cityPhrase + ")";
                }
                expect += JEXL_AND_OP + "!((" + Constants.ANY_FIELD + RE_OP + "'.*ica'" + JEXL_OR_OP + CityField.CONTINENT.name() + EQ_OP + "'north america'))";
                String plan = getPlan(query, true, true);
                assertPlanEquals(expect, plan);
                
                // Test the plan sans value expansion
                expect = CityField.CITY.name() + cityPhrase;
                if (city.name().equals("london")) {
                    expect = "(" + expect + JEXL_OR_OP + CityField.STATE.name() + cityPhrase + ")";
                }
                expect += JEXL_AND_OP + "!((" + Constants.ANY_FIELD + RE_OP + "'.*ica'" + JEXL_OR_OP + CityField.CONTINENT.name() + RE_OP + "'.*ica'))";
                plan = getPlan(query, true, false);
                assertPlanEquals(expect, plan);
                
                // Test the plan sans field expansion
                expect = Constants.ANY_FIELD + cityPhrase + JEXL_AND_OP + "!((" + Constants.ANY_FIELD + RE_OP + "'.*ica'" + JEXL_OR_OP + Constants.ANY_FIELD
                                + EQ_OP + "'north america'))";
                plan = getPlan(query, false, true);
                assertPlanEquals(expect, plan);
                
                // test running the query
                expect = anyCity + AND_OP + negReg;
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
                
                // Test the plan with all expansions
                String expect = CityField.CITY.name() + cityPhrase;
                if (city.name().equals("london")) {
                    expect += JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
                }
                expect += JEXL_OR_OP + "(((!(" + Constants.ANY_FIELD + RE_OP + "'.*ica')" + JEXL_AND_OP + "!(" + CityField.CONTINENT.name() + EQ_OP
                                + "'north america'))))";
                String plan = getPlan(query, true, true);
                assertPlanEquals(expect, plan);
                
                // Test the plan sans value expansion
                expect = CityField.CITY.name() + cityPhrase;
                if (city.name().equals("london")) {
                    expect += JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
                }
                expect += JEXL_OR_OP + "(((!(" + Constants.ANY_FIELD + RE_OP + "'.*ica')" + JEXL_AND_OP + "!(" + CityField.CONTINENT.name() + RE_OP
                                + "'.*ica'))))";
                plan = getPlan(query, true, false);
                assertPlanEquals(expect, plan);
                
                // Test the plan sans field expansion
                expect = Constants.ANY_FIELD + cityPhrase + JEXL_OR_OP + "(((!(" + Constants.ANY_FIELD + RE_OP + "'.*ica')" + JEXL_AND_OP + "!("
                                + Constants.ANY_FIELD + EQ_OP + "'north america'))))";
                plan = getPlan(query, false, true);
                assertPlanEquals(expect, plan);
                
                // test running the query
                expect = anyCity + OR_OP + negReg;
                runTest(query, expect);
            }
        } finally {
            this.logic.setFullTableScanEnabled(false);
        }
    }
    
    @Test
    public void testNegRegexOrDisallowedExpansion() throws Exception {
        String regPhrase = RN_OP + "'.*ica'";
        String negReg = this.dataManager.convertAnyField(regPhrase, AND_OP);
        try {
            this.logic.setFullTableScanEnabled(true);
            this.logic.setExpandUnfieldedNegations(false);
            for (final TestCities city : TestCities.values()) {
                String cityPhrase = EQ_OP + "'" + city.name() + "'";
                String anyCity = this.dataManager.convertAnyField(cityPhrase);
                String query = Constants.ANY_FIELD + cityPhrase + OR_OP + Constants.ANY_FIELD + regPhrase;
                
                // Test the plan with all expansions
                String expect = CityField.CITY.name() + cityPhrase;
                if (city.name().equals("london")) {
                    expect += JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
                }
                expect += JEXL_OR_OP + "!(" + Constants.ANY_FIELD + RE_OP + "'.*ica')";
                String plan = getPlan(query, true, true);
                assertPlanEquals(expect, plan);
                
                // Test the plan sans value expansion
                expect = CityField.CITY.name() + cityPhrase;
                if (city.name().equals("london")) {
                    expect += JEXL_OR_OP + CityField.STATE.name() + cityPhrase;
                }
                expect += JEXL_OR_OP + "!(" + Constants.ANY_FIELD + RE_OP + "'.*ica')";
                plan = getPlan(query, true, false);
                assertPlanEquals(expect, plan);
                
                // Test the plan sans field expansion
                expect = Constants.ANY_FIELD + cityPhrase + JEXL_OR_OP + "!(" + Constants.ANY_FIELD + RE_OP + "'.*ica')";
                plan = getPlan(query, false, true);
                assertPlanEquals(expect, plan);
                
                // test running the query
                expect = anyCity + OR_OP + negReg;
                runTest(query, expect);
            }
        } finally {
            this.logic.setFullTableScanEnabled(false);
        }
    }
    
    @Test
    public void testRegexPushdownAnyfield() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String anyRo = this.dataManager.convertAnyField(roPhrase);
        String oPhrase = RE_OP + "'.*a'";
        String anyO = this.dataManager.convertAnyField(oPhrase);
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + Constants.ANY_FIELD + oPhrase;
        
        RegexPushdownTransformRule rule = new RegexPushdownTransformRule();
        rule.setRegexPatterns(Arrays.asList("\\.\\*[0-9a-zA-Z]", "[0-9a-zA-Z]\\.\\*"));
        ((DefaultQueryPlanner) (logic.getQueryPlanner())).setTransformRules(Collections.singletonList(rule));
        
        // Test the plan with all expansions
        try {
            String plan = getPlan(query, true, true);
            fail("Expected failure for regex pushdown on anyfield");
        } catch (Exception e) {
            // expected
        }
        
        try {
            String plan = getPlan(query, false, true);
            fail("Expected failure for regex pushdown on anyfield");
        } catch (Exception e) {
            // expected
        }
        
        try {
            String plan = getPlan(query, true, false);
            fail("Expected failure for regex pushdown on anyfield");
        } catch (Exception e) {
            // expected
        }
        
        // test running the query
        String expect = anyRo + AND_OP + anyO;
        try {
            runTest(query, expect);
            fail("Expected failure for regex pushdown on anyfield");
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testRegexPushdownField() throws Exception {
        String roPhrase = RE_OP + "'ro.*'";
        String anyRo = this.dataManager.convertAnyField(roPhrase);
        String yPhrase = RE_OP + "'.*y'";
        String cityY = CityField.COUNTRY + yPhrase;
        String query = Constants.ANY_FIELD + roPhrase + AND_OP + CityField.COUNTRY + yPhrase;
        
        RegexPushdownTransformRule rule = new RegexPushdownTransformRule();
        rule.setRegexPatterns(Arrays.asList("\\.\\*[0-9a-zA-Z]", "[0-9a-zA-Z]\\.\\*"));
        ((DefaultQueryPlanner) (logic.getQueryPlanner())).setTransformRules(Collections.singletonList(rule));
        
        // Test the plan with all expansions
        String expect = CityField.CITY.name() + EQ_OP + "'rome'" + JEXL_AND_OP + "((_Eval_ = true) && (" + cityY + "))";
        String plan = getPlan(query, true, true);
        assertPlanEquals(expect, plan);
        
        expect = Constants.ANY_FIELD + EQ_OP + "'rome'" + JEXL_AND_OP + "((_Eval_ = true) && (" + cityY + "))";
        plan = getPlan(query, false, true);
        assertPlanEquals(expect, plan);
        
        expect = CityField.CITY + roPhrase + JEXL_AND_OP + "((_Eval_ = true) && (" + cityY + "))";
        plan = getPlan(query, true, false);
        assertPlanEquals(expect, plan);
        
        // test running the query
        expect = anyRo + AND_OP + cityY;
        runTest(query, expect);
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
