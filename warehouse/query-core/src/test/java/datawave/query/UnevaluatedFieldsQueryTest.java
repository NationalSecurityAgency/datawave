package datawave.query;

import datawave.query.exceptions.InvalidQueryException;
import datawave.query.testframework.AbstractFields;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;

/**
 * Query unit tests for settings fields that are to be not evaluated for a query. The code base treats an unevaluted field in the same as an index only field.
 * Create an index only field in oder to test this.
 */
public class UnevaluatedFieldsQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(UnevaluatedFieldsQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig fldConfig = new UnevaluatedCityFields();
        dataTypes.add(new CitiesDataType(CityEntry.generic, fldConfig));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public UnevaluatedFieldsQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @After
    public void cleanup() {
        this.logic.setUnevaluatedFields(Collections.emptyList());
    }
    
    @Test
    public void testUnEval() throws Exception {
        log.info("------  testUnEval  ------");
        String code = "'USA'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.CODE.name() + EQ_OP + code;
            runTest(query, query);
        }
    }
    
    @Test
    public void testNoIndex() throws Exception {
        log.info("------  testNoIndex  ------");
        String co = "'usa'";
        
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.COUNTRY.name() + EQ_OP + co;
            try {
                runTest(query, query);
                Assert.fail("exception condition expected");
            } catch (InvalidQueryException iqe) {
                // expected
            }
        }
    }
    
    @Test
    public void testAndNot() throws Exception {
        log.info("------  testAndNot  ------");
        String code = "'usa'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CODE.name() + EQ_OP + code + AND_OP + "!(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + ")";
            runTest(query, query);
        }
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
        
        this.logic.setUnevaluatedFields(UnevaluatedCityFields.indexOnly);
    }
    
    // ============================================
    // private methods
    
    private static class UnevaluatedCityFields extends AbstractFields {
        
        private static final Collection<String> index = Arrays.asList(CityField.CITY.name(), CityField.STATE.name(), CityField.CODE.name());
        private static final List<String> indexOnly = Arrays.asList(CityField.CODE.name(), CityField.COUNTRY.name());
        private static final Collection<String> reverse = new HashSet<>();
        private static final Collection<String> multivalue = Arrays.asList(CityField.CITY.name(), CityField.STATE.name());
        
        private static final Collection<Set<String>> composite = new HashSet<>();
        private static final Collection<Set<String>> virtual = new HashSet<>();
        
        public UnevaluatedCityFields() {
            super(index, indexOnly, reverse, multivalue, composite, virtual);
        }
        
        @Override
        public String toString() {
            return "GenericCityFields{" + super.toString() + "}";
        }
        
    }
}
