package datawave.query.sequentialscheduler;

import datawave.query.tables.CountingShardQueryLogic;
import datawave.query.testframework.*;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static datawave.query.testframework.RawDataManager.*;

/**
 * Tests that use the {@link CountingShardQueryLogic}.
 */
public class CountQueryTest extends UseSequentialScheduler {
    
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
