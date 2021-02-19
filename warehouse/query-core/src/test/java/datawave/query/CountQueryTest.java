package datawave.query;

import datawave.query.tables.CountingShardQueryLogic;
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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.NE_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

/**
 * Tests that use the {@link CountingShardQueryLogic}.
 */
public class CountQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(CountQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.CODE.name());
        Set<String> virt = new HashSet<>(Arrays.asList(CityField.CITY.name(), CityField.CONTINENT.name()));
        generic.removeVirtualField(virt);
        for (String idx : generic.getIndexFields()) {
            generic.addReverseIndexField(idx);
        }
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public CountQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testRegex() throws Exception {
        log.info("------  testRegex  ------");
        
        String state = "'mISs.*'";
        String query = CityField.CODE.name() + EQ_OP + "'usA'" + AND_OP + CityField.STATE.name() + RE_OP + state;
        runCountTest(query);
    }
    
    @Test
    public void testRegexMulti() throws Exception {
        log.info("------  testRegexMulti  ------");
        
        String state = "'m.*si.*'";
        String query = CityField.CODE.name() + EQ_OP + "'usA'" + AND_OP + CityField.STATE.name() + RE_OP + state;
        runCountTest(query);
    }
    
    @Test
    public void testEqual() throws Exception {
        log.info("------  testEqual  ------");
        
        String state = "'Missouri'";
        String query = CityField.STATE.name() + EQ_OP + state;
        runCountTest(query);
    }
    
    @Test
    public void testNotEqual() throws Exception {
        log.info("------  testNotEqual  ------");
        
        String state = "'Missouri'";
        String query = CityField.STATE.name() + NE_OP + state + AND_OP + CityField.CONTINENT.name() + RE_OP + "'north.*'";
        runCountTest(query);
    }
    
    @Test
    public void testOr() throws Exception {
        log.info("------  testOr  ------");
        
        String city = "'paris'";
        String fra = "'frA'";
        String usa = "'UsA'";
        String ita = "'iTa'";
        String query = "(" + CityField.CITY.name() + EQ_OP + city + AND_OP + CityField.CODE.name() + EQ_OP + fra + ")" + OR_OP + "(" + CityField.CITY.name()
                        + EQ_OP + city + AND_OP + CityField.CODE.name() + EQ_OP + usa + ")" + OR_OP + "(" + CityField.CITY.name() + EQ_OP + city + AND_OP
                        + CityField.CODE.name() + EQ_OP + ita + ")";
        runCountTest(query);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
