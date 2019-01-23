package datawave.query;

import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.GenericCityFields;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class TextFunctionQueryTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(TextFunctionQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        connector = helper.loadTables(log);
    }
    
    public TextFunctionQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testAnyFieldText() throws Exception {
        log.info("------  testAnyFieldText  ------");
        String code = "europe";
        // must be same case as original value in event
        String state = "Lazio";
        String phrase = EQ_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#TEXT(" + state + ")";
        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
        
        // testing that incorrect case misses results
        state = "lazio";
        query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#TEXT(" + state + ")";
        // should return the empty set
        runTestQuery(Collections.EMPTY_SET, query);
    }
    
    @Test
    public void testExplicitAnyFieldText() throws Exception {
        log.info("------  testExplicitAnyFieldText  ------");
        String code = "europe";
        String state = "Lazio";
        String phrase = EQ_OP + "'" + state + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#TEXT(" + Constants.ANY_FIELD + "," + state + ")";
        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testMultiFieldText() throws Exception {
        log.info("------  testMultiFieldText  ------");
        String code = "europe";
        String state1 = "Lazio";
        String state2 = "London";
        String phrase1 = EQ_OP + "'" + state1 + "'";
        String phrase2 = EQ_OP + "'" + state2 + "'";
        String query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#TEXT(OR, STATE," + state1 + ", STATE, " + state2 + ")";
        String expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + "( STATE" + phrase1 + OR_OP + "STATE" + phrase2 + " )";
        runTest(query, expect);
        
        // testing that incorrect case misses results
        state2 = "london";
        query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#TEXT(OR, STATE," + state1 + ", STATE, " + state2 + ")";
        expect = CityField.CONTINENT.name() + EQ_OP + "'" + code + "'" + AND_OP + "STATE" + phrase1;
        // should return the empty set
        runTest(query, expect);
        
        // testing that incorrect case misses results
        state1 = "lazio";
        query = CityField.CONTINENT.name() + ":\"" + code + "\"" + AND_OP + "#TEXT(OR, STATE," + state1 + ", STATE, " + state2 + ")";
        // should return the empty set
        runTestQuery(Collections.EMPTY_SET, query);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
        
        this.logic.setParser(new LuceneToJexlQueryParser());
    }
}
