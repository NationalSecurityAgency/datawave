package datawave.query;

import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.IpAddrFields;
import datawave.query.testframework.IpAddressDataType;
import datawave.query.testframework.IpAddressDataType.IpAddrEntry;
import datawave.query.testframework.IpAddressDataType.IpAddrField;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.GTE_OP;
import static datawave.query.testframework.RawDataManager.LTE_OP;
import static datawave.query.testframework.RawDataManager.NE_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class IpAddressQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(IpAddressQueryTest.class);
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        FieldConfig fieldInfo = new IpAddrFields();
        DataTypeHadoopConfig dataType = new IpAddressDataType(IpAddrEntry.ipbase, fieldInfo);
        accumuloSetup.setData(FileType.CSV, dataType);
        connector = accumuloSetup.loadTables(log);
    }
    
    public IpAddressQueryTest() {
        super(IpAddressDataType.getManager());
    }
    
    @Test
    public void testSingleValue() throws Exception {
        String query = IpAddrField.PUBLIC_IP.name() + EQ_OP + "'8.121.11.129'";
        runTest(query, query);
    }
    
    @Test
    public void testOr() throws Exception {
        String query = IpAddrField.PUBLIC_IP.name() + EQ_OP + "'9.9.80.122'" + OR_OP + IpAddrField.PRIVATE_IP.name() + EQ_OP + "'33.33.33.55'";
        runTest(query, query);
    }
    
    @Test
    public void testRange() throws Exception {
        String query = "((_Bounded_ = true) && (" + IpAddrField.PUBLIC_IP.name() + GTE_OP + "'9.9.9.9'" + AND_OP + IpAddrField.PUBLIC_IP.name() + LTE_OP
                        + "'9.9.40.1'))";
        runTest(query, query);
    }
    
    @Test
    public void testRangeWithRegexField() throws Exception {
        String query = "((_Bounded_ = true) && (" + IpAddrField.PUBLIC_IP.name() + GTE_OP + "'9.9.9.9'" + AND_OP + IpAddrField.PUBLIC_IP.name() + LTE_OP
                        + "'9.9.40.1'))" + AND_OP + IpAddrField.PLANET.name() + RE_OP + "'m.*'";
        runTest(query, query);
    }
    
    @Test
    public void testRangeWithNotEq() throws Exception {
        String query = "((_Bounded_ = true) && (" + IpAddrField.PRIVATE_IP.name() + GTE_OP + "'20.20.20.20'" + AND_OP + IpAddrField.PRIVATE_IP.name() + LTE_OP
                        + "'30.30.30.30'" + "))" + AND_OP + IpAddrField.LOCATION.name() + NE_OP + "'paris'";
        runTest(query, query);
    }
    
    @Test
    public void testMultiRange() throws Exception {
        String query = "((_Bounded_ = true) && (" + IpAddrField.PRIVATE_IP.name() + GTE_OP + "'20.20.20.20'" + AND_OP + IpAddrField.PRIVATE_IP.name() + LTE_OP
                        + "'22.90.90.200')" + ")" + OR_OP + "((((_Bounded_ = true) && (" + IpAddrField.PRIVATE_IP.name() + GTE_OP + "'33.60.60.60'" + ")"
                        + AND_OP + IpAddrField.PRIVATE_IP.name() + LTE_OP + "'33.100.100.200'" + ")))";
        runTest(query, query);
    }
    
    @Test
    public void testAnyFieldRegex() throws Exception {
        String phrase = RE_OP + "'33\\.90\\..*'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testRegexClassA() throws Exception {
        String phrase = RE_OP + "'9\\..*\\..*\\..*'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testRegexClassB() throws Exception {
        String phrase = RE_OP + "'8\\..8\\..*\\..*'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    @Test
    public void testRegexClassC() throws Exception {
        String phrase = RE_OP + "'8\\.8\\.90\\..*'";
        String query = Constants.ANY_FIELD + phrase;
        String expect = this.dataManager.convertAnyField(phrase);
        runTest(query, expect);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = IpAddressDataType.getTestAuths();
        this.documentKey = IpAddressDataType.IpAddrField.EVENT_ID.name();
    }
}
