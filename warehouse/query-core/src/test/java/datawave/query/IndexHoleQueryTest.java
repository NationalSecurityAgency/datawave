package datawave.query;

import datawave.query.config.IndexHole;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.testframework.AbstractFields;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.BaseShardIdRange;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CitiesDataType.CityEntry;
import datawave.query.testframework.CitiesDataType.CityField;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.ShardIdValues;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;

/**
 * The index hole provides the means of using the entries in the event when indexes are missing for a range.
 */
public class IndexHoleQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(IndexHoleQueryTest.class);
    
    private static final List<IndexHole> INDEX_HOLE = new ArrayList<>();
    static {
        String[] dateHole = new String[] {BaseShardIdRange.DATE_2015_0404.getDateStr(), BaseShardIdRange.DATE_2015_0505.getDateStr()};
        IndexHole hole = new IndexHole(dateHole, new String[] {"us", "ut"});
        INDEX_HOLE.add(hole);
    }
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        // add two datatypes that contain different indexes - this will create an index hole
        FieldConfig noHoles = new NoHoleFields();
        dataTypes.add(new CitiesDataType(CityEntry.generic, noHoles));
        
        // because the CODE field is not indexed the type must be specified in the configuration
        // without the type in the configuration the test will fail because the metadata will not be contain the type
        FieldConfig holes = new HoleFields();
        dataTypes.add(new CitiesDataType(CityEntry.hole, holes));
        
        accumuloSetup.setData(FileType.CSV, dataTypes);
        connector = accumuloSetup.loadTables(log);
    }
    
    public IndexHoleQueryTest() {
        super(CitiesDataType.getManager());
    }
    
    @Test
    public void testFieldOnly() throws Exception {
        log.info("------  testFieldOnly  ------");
        String usa = "'Usa'";
        String query = CityField.CODE.name() + EQ_OP + usa;
        // date range should not include non-index range for hole
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0606.getDateStr());
        Date end = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_1111.getDateStr());
        runTest(query, query, start, end);
    }
    
    @Test(expected = FullTableScansDisallowedException.class)
    public void testErrorFieldOnly() throws Exception {
        log.info("------  testErrorFieldOnly  ------");
        String usa = "'uSa'";
        String query = CityField.CODE.name() + EQ_OP + usa;
        // setting the index hole creates an invalid query
        this.logic.setIndexHoles(INDEX_HOLE);
        runTest(query, query);
    }
    
    @Test
    public void testHoleOnly() throws Exception {
        log.info("------  testHoleOnly  ------");
        String usa = "'uSa'";
        String rome = "'rOme'";
        String query = CityField.CODE.name() + EQ_OP + usa + AND_OP + CityField.CITY.name() + EQ_OP + rome;
        this.logic.setIndexHoles(INDEX_HOLE);
        // set the date range to cover just the index hole
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0505.getDateStr());
        Date end = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0505.getDateStr());
        runTest(query, query, start, end);
    }
    
    @Test
    public void testWithoutHole() throws Exception {
        log.info("------  testWithoutHole  ------");
        String usa = "'usA'";
        String rome = "'rOme'";
        String query = CityField.CODE.name() + EQ_OP + usa + AND_OP + CityField.CITY.name() + EQ_OP + rome;
        this.logic.setIndexHoles(INDEX_HOLE);
        // set the date range to exclude the index hole
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0808.getDateStr());
        Date end = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0808.getDateStr());
        runTest(query, query, start, end);
    }
    
    @Test
    public void testHole() throws Exception {
        log.info("------  testHole  ------");
        //
        String usa = "'usA'";
        String rome = "'roMe'";
        String query = CityField.CODE.name() + EQ_OP + usa + AND_OP + CityField.CITY.name() + EQ_OP + rome;
        this.logic.setIndexHoles(INDEX_HOLE);
        // results should consist of entries from non-indexed for hole datatype and indexed entries from generic datatype
        Date start = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0505.getDateStr());
        Date end = ShardIdValues.convertShardToDate(BaseShardIdRange.DATE_2015_0808.getDateStr());
        runTest(query, query, start, end);
    }
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }
    
    private static class NoHoleFields extends AbstractFields {
        
        private static final Collection<String> index = new HashSet<>();
        private static final Collection<String> indexOnly = new HashSet<>();
        private static final Collection<String> reverse = new HashSet<>();
        private static final Collection<String> multivalue = new HashSet<>();
        
        private static final Collection<Set<String>> composite = new HashSet<>();
        private static final Collection<Set<String>> virtual = new HashSet<>();
        
        static {
            index.add(CityField.CITY.name());
            index.add(CityField.STATE.name());
            index.add(CityField.CODE.name());
            reverse.addAll(index);
        }
        
        public NoHoleFields() {
            super(index, indexOnly, reverse, multivalue, composite, virtual);
        }
        
        @Override
        public String toString() {
            return "NoHoleFields{" + super.toString() + "}";
        }
    }
    
    private static class HoleFields extends AbstractFields {
        
        private static final Collection<String> index = new HashSet<>();
        private static final Collection<String> indexOnly = new HashSet<>();
        private static final Collection<String> reverse = new HashSet<>();
        private static final Collection<String> multivalue = new HashSet<>();
        
        private static final Collection<Set<String>> composite = new HashSet<>();
        private static final Collection<Set<String>> virtual = new HashSet<>();
        
        static {
            // index city and state but not code
            index.add(CityField.CITY.name());
            index.add(CityField.STATE.name());
            reverse.addAll(index);
        }
        
        public HoleFields() {
            super(index, indexOnly, reverse, multivalue, composite, virtual);
        }
        
        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{" + super.toString() + "}";
        }
    }
}
