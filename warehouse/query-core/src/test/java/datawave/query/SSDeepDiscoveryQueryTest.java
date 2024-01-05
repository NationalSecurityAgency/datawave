package datawave.query;

import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.GenericSSDeepFields;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.Collection;

public class SSDeepDiscoveryQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(SSDeepDiscoveryQueryTest.class);

    @BeforeClass
    public static void filterSetup() throws Exception() {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericSSDeepFields();


    }
}
