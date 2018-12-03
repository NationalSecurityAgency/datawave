package datawave.query.testframework;

import datawave.ingest.json.util.JsonObjectFlattener;
import datawave.query.SimpleFlattenQueryTest;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;

public abstract class AbstractFlattenQuery { // extends AbstractFunctionalQuery {

    // private static final Logger log = Logger.getLogger(SimpleFlattenQueryTest.class);
    //
    // @BeforeClass
    // public static void filterSetup() throws Exception {
    // Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
    // FieldConfig indexes = getIndexConfig();
    // FlattenData data = getFlattenData();
    // Flat
    // FlattenDataType dt = new FlattenDataType(FlattenDataType.FlattenEntry.cityFlatten, indexes, data);
    // dataTypes.add(dt);
    //
    // final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes, FileLoaderFactory.FileType.JSON);
    // connector = helper.loadTables(log);
    // }
    //
    //
    // protected FieldConfig getFieldConfig();
    
}
