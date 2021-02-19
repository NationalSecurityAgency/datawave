package datawave.query;

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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GT_OP;
import static datawave.query.testframework.RawDataManager.LT_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class CompositeQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(CompositeQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.CODE.name());
        Set<String> cityNum = new HashSet<>();
        cityNum.add(CityField.CITY.name());
        cityNum.add(CityField.NUM.name());
        generic.addCompositeField(cityNum);
        Set<String> stateCode = new HashSet<>();
        stateCode.add(CityField.STATE.name());
        stateCode.add(CityField.CODE.name());
        generic.addCompositeField(stateCode);
        Set<String> stateCont = new HashSet<>();
        stateCont.add(CityField.STATE.name());
        stateCont.add(CityField.CONTINENT.name());
        generic.addCompositeField(stateCont);
        for (String idx : generic.getIndexFields()) {
            generic.addReverseIndexField(idx);
        }
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public CompositeQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testEquivalentComposites() throws Exception {
        log.info("------  testEquivalentComposites  ------");
        String state = "'MissOuri'";
        String num = "110";
        
        for (TestCities city : TestCities.values()) {
            // create multiple queries that use composites that resolve to the same results
            String qState = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + EQ_OP + state;
            String qNum = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.NUM.name() + EQ_OP + num;
            
            runTest(qState, qNum);
            runTest(qNum, qState);
        }
    }
    
    @Test
    public void testRegexWithTerm() throws Exception {
        log.info("------  testRegexWithTerm  ------");
        String state = "'Miss.*'";
        String code = "'uSa'";
        String cont = "'NorTh AmeRica'";
        
        String qState = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.CONTINENT.name() + EQ_OP + cont + AND_OP + CityField.STATE.name() + RE_OP
                        + state;
        runTest(qState, qState);
    }
    
    @Test
    public void testRegexWithRange() throws Exception {
        log.info("------  testRegexWithRange  ------");
        String state = "'Miss.*'";
        String code = "'uSa'";
        
        String qState = CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.NUM.name() + GT_OP + "90" + AND_OP + CityField.NUM.name() + LT_OP + "135"
                        + AND_OP + CityField.STATE + RE_OP + state;
        runTest(qState, qState);
    }
    
    @Test
    public void testMultiRegex() throws Exception {
        log.info("------  testMultiRegex  ------");
        String state = "'Miss.*'";
        String code = "'.*a'";
        
        String qState = CityField.CODE.name() + RE_OP + code + AND_OP + CityField.NUM.name() + GT_OP + "90" + AND_OP + CityField.NUM.name() + LT_OP + "135"
                        + AND_OP + CityField.STATE.name() + RE_OP + state;
        this.logic.setQueryThreads(1);
        runTest(qState, qState);
    }
    
    @Test
    public void testOrTerm() throws Exception {
        log.info("------  testOrTerm  ------");
        String state = "'Miss.*'";
        String code = "'uSa'";
        String cont = "'noRTH AMERica'";
        
        String qState = "(" + CityField.CODE.name() + EQ_OP + code + AND_OP + CityField.CONTINENT.name() + EQ_OP + cont + AND_OP + CityField.STATE.name()
                        + RE_OP + state + ")" + OR_OP + CityField.CODE.name() + EQ_OP + "'ita'";
        runTest(qState, qState);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
