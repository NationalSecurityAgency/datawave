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

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GTE_OP;
import static datawave.query.testframework.RawDataManager.LTE_OP;
import static datawave.query.testframework.RawDataManager.LT_OP;
import static datawave.query.testframework.RawDataManager.NE_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

public class CompoundJexlQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(CompoundJexlQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CityField.NUM.name());
        generic.addReverseIndexField(CityField.NUM.name());
        dataTypes.add(new CitiesDataType(CityEntry.generic, generic));
        dataTypes.add(new CitiesDataType((CityEntry.italy), generic));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public CompoundJexlQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testOr_Or() throws Exception {
        log.info("------  testOr_Or  ------");
        String cont = "'eurOpe'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP + "(" + CityField.CONTINENT.name() + EQ_OP + cont + OR_OP
                            + CityField.NUM.name() + EQ_OP + "120)";
            runTest(query, query);
        }
    }
    
    @Test
    public void testOrOr_And() throws Exception {
        log.info("------  testOrOr_And  ------");
        String cont = "'eurOpe'";
        String ohio = "'oHio'";
        String mizzo = "'miSSouri'";
        
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.STATE.name() + EQ_OP + ohio + OR_OP + CityField.STATE.name() + EQ_OP + mizzo + OR_OP + CityField.CONTINENT.name()
                            + EQ_OP + cont + ")" + AND_OP + "(" + CityField.CITY.name() + NE_OP + "'" + city.name() + "')";
            runTest(query, query);
        }
    }
    
    @Test
    public void testOrOr_And_And() throws Exception {
        log.info("------  testOrOr_And_And  ------");
        String cont = "'eurOpe'";
        String ohio = "'oHio'";
        String mizzo = "'miSSouri'";
        String country = "'italy'";
        
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.STATE.name() + EQ_OP + ohio + OR_OP + CityField.STATE.name() + EQ_OP + mizzo + OR_OP + "("
                            + CityField.COUNTRY.name() + EQ_OP + country + AND_OP + CityField.CONTINENT.name() + EQ_OP + cont + "))" + AND_OP + "("
                            + CityField.CITY.name() + NE_OP + "'" + city.name() + "')";
            runTest(query, query);
        }
    }
    
    @Test
    public void testOr_Or_And_And() throws Exception {
        log.info("------  testOr_Or_And_And  ------");
        String cont = "'eurOpe'";
        String ohio = "'oHio'";
        String mizzo = "'miSSouri'";
        String country = "'italy'";
        
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.STATE.name() + EQ_OP + ohio + OR_OP + "(" + CityField.STATE.name() + EQ_OP + mizzo + OR_OP + "("
                            + CityField.COUNTRY.name() + EQ_OP + country + AND_OP + CityField.CONTINENT.name() + EQ_OP + cont + ")))" + AND_OP + "("
                            + CityField.CITY.name() + NE_OP + "'" + city.name() + "')";
            runTest(query, query);
        }
    }
    
    @Test
    public void testOr_And() throws Exception {
        log.info("------  testOrOr  ------");
        String cont = "'eurOpe'";
        String code = "'uSa'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP + CityField.CONTINENT.name() + EQ_OP + cont + ")" + AND_OP
                            + CityField.CODE.name() + NE_OP + code;
            runTest(query, query);
        }
    }
    
    @Test
    public void testAnd_Or() throws Exception {
        log.info("------  testAnd_Or  ------");
        String code = "'uSa'";
        String state = "'miSSouri'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CODE.name() + EQ_OP + code + AND_OP + "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP
                            + CityField.STATE.name() + EQ_OP + state + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testAndNot_Or() throws Exception {
        log.info("------  testAndOr  ------");
        String code = "'ita'";
        String mizzo = "'MissouRi'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + " not (" + CityField.STATE.name() + EQ_OP + mizzo + OR_OP
                            + CityField.CODE.name() + EQ_OP + code + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testOr_And_Or() throws Exception {
        log.info("------  testOrAndOr  ------");
        String cont = "'europe'";
        String state = "'miSSissippi'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP + CityField.STATE.name() + EQ_OP + state + ")" + AND_OP + "("
                            + CityField.CONTINENT.name() + EQ_OP + cont + OR_OP + CityField.NUM.name() + LT_OP + "104)";
            runTest(query, query);
        }
    }
    
    @Test
    public void testOrAnd_Or() throws Exception {
        log.info("------  testOrAndOr  ------");
        String cont = "'europe'";
        String state = "'miSSissippi'";
        String code = "50";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + OR_OP + CityField.STATE.name() + EQ_OP + state + AND_OP
                            + CityField.CONTINENT.name() + EQ_OP + cont + ")" + OR_OP + "(" + CityField.NUM.name() + EQ_OP + code + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testMultiOr() throws Exception {
        log.info("------  testMulti  ------");
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "((" + CityField.NUM.name() + " == 100 or "
                            + CityField.NUM.name() + EQ_OP + "110" + OR_OP + CityField.NUM.name() + EQ_OP + "120" + OR_OP + CityField.NUM.name() + " < 20)"
                            + " or (" + CityField.COUNTRY.name() + EQ_OP + "'FrAnce'" + OR_OP + CityField.COUNTRY.name() + EQ_OP + "'iTaLy'" + OR_OP
                            + CityField.COUNTRY.name() + EQ_OP + "'UniTED kIngdom'" + "))";
            runTest(query, query);
        }
    }
    
    @Test
    public void testAnd_Or_And() throws Exception {
        log.info("------  testAnd_Or_And  ------");
        String cont = "'euroPe'";
        String state = "'miSSouri'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.STATE.name() + EQ_OP + state + ")" + OR_OP + "("
                            + CityField.CONTINENT.name() + EQ_OP + cont + AND_OP + CityField.CITY.name() + EQ_OP + "'" + city.name() + "')";
            runTest(query, query);
        }
    }
    
    @Test
    public void testAndAnd_Or_And() throws Exception {
        log.info("------  testAndAnd_Or_And  ------");
        String cont = "'euroPe'";
        String state = "'miSSouri'";
        String code = "'usA'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.CODE.name() + EQ_OP + code + AND_OP
                            + CityField.STATE.name() + EQ_OP + state + ")" + OR_OP + "(" + CityField.CONTINENT.name() + EQ_OP + cont + AND_OP
                            + CityField.CITY.name() + EQ_OP + "'" + city.name() + "')";
            runTest(query, query);
        }
    }
    
    @Test
    public void testAndAnd_Or_And_Or() throws Exception {
        log.info("------  testAndAnd_Or_And_Or  ------");
        String cont = "'euroPe'";
        String state = "'miSSouri'";
        String ohio = "'oHIo'";
        String code = "'usA'";
        for (final TestCities city : TestCities.values()) {
            String query = "(" + CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + CityField.CODE.name() + EQ_OP + code + AND_OP
                            + CityField.STATE.name() + EQ_OP + state + ")" + OR_OP + "(" + CityField.CONTINENT.name() + EQ_OP + cont + AND_OP
                            + CityField.CITY.name() + EQ_OP + "'" + city.name() + "')" + OR_OP + "(" + CityField.STATE.name() + EQ_OP + ohio + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testAnd_OrOr() throws Exception {
        log.info("------  testAnd_OrOr  ------");
        String state = "'miSSouri'";
        String country = "'united states'";
        String num = "100";
        String code = "'iTa'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.CODE.name() + EQ_OP + code + OR_OP
                            + CityField.COUNTRY.name() + EQ_OP + country + OR_OP + CityField.NUM.name() + EQ_OP + num + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testAnd_Or_And_Or_And() throws Exception {
        log.info("------  testAnd_Or_And_Or_And  ------");
        String state = "'miSSouri'";
        String num = "100";
        String code = "'iTa'";
        for (final TestCities city : TestCities.values()) {
            String cStr = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'";
            String query = "(" + cStr + AND_OP + CityField.CODE.name() + EQ_OP + code + ")" + OR_OP + "(" + cStr + AND_OP + CityField.STATE.name() + EQ_OP
                            + state + ")" + OR_OP + "(" + cStr + AND_OP + CityField.NUM.name() + EQ_OP + num + ")";
            runTest(query, query);
        }
    }
    
    @Test
    public void testNumericAndRange() throws Exception {
        log.info("------  testNumericAndRange  ------");
        String query = "(" + CityField.NUM.name() + GTE_OP + "30)" + AND_OP + "(" + CityField.NUM.name() + LTE_OP + "105)";
        runTest("((_Bounded_ = true) && (" + query + "))", query);
    }
    
    @Test
    public void testAnd_OrWithComposite() throws Exception {
        log.info("------  testErrorAnd_OrWithComposite  ------");
        String state = "'miSSouri'";
        String code = "'iTa'";
        for (final TestCities city : TestCities.values()) {
            String query = CityField.CITY.name() + EQ_OP + "'" + city.name() + "'" + AND_OP + "(" + CityField.CODE.name() + EQ_OP + code + OR_OP
                            + CityField.STATE.name() + EQ_OP + state + ")";
            runTest(query, query);
        }
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CityField.EVENT_ID.name();
    }
}
