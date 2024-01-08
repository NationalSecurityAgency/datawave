package datawave.query.tables.ssdeep;

import datawave.helpers.PrintUtility;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericSSDeepFields;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class SSDeepDiscoveryQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(SSDeepDiscoveryQueryTest.class);

    @BeforeClass
    public static void filterSetup() throws Exception {
        log.setLevel(Level.DEBUG);
        Logger printLog = Logger.getLogger(PrintUtility.class);
        printLog.setLevel(Level.DEBUG);

        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericSSDeepFields();
        dataTypes.add(new SSDeepDataType(SSDeepDataType.SSDeepEntry.ssdeep, generic));

        accumuloSetup.setData(FileType.CSV, dataTypes);
        client = accumuloSetup.loadTables(log);
    }

    public SSDeepDiscoveryQueryTest() {
        super(SSDeepDataType.getManager());
    }

    @Test
    public void testDiscovery() throws Exception {
        log.info("------ testSSDeepDiscovery ------");
    }

    protected void testInit() {
        this.auths = SSDeepDataType.getTestAuths();
        this.documentKey = SSDeepDataType.SSDeepField.EVENT_ID.name();
    }
}
