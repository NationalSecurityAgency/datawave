package datawave.query;

import datawave.data.normalizer.Normalizer;
import datawave.ingest.json.util.JsonObjectFlattener.FlattenMode;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.testframework.AbstractFields;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.FlattenData;
import datawave.query.testframework.FlattenDataType;
import datawave.query.testframework.FlattenDataType.FlattenBaseFields;
import datawave.query.testframework.RawDataManager;
import datawave.query.testframework.RawMetaData;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;

/**
 * Test cases for flatten mode {@link FlattenMode@SIMPLE}.
 */
public class SimpleFlattenQueryTest extends AbstractFunctionalQuery {
    
    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();
    
    private static final Logger log = Logger.getLogger(SimpleFlattenQueryTest.class);
    
    private static final FlattenMode flatMode = FlattenMode.SIMPLE;
    private static final FlattenDataType flatten;
    // each flatten mode will have a separate manager
    private static final RawDataManager manager;
    
    static {
        FieldConfig indexes = new SimpleIndexing();
        FlattenData data = new FlattenData(SimpleField.STARTDATE.name(), SimpleField.EVENTID.name(), flatMode, SimpleField.headers, SimpleField.metadataMapping);
        manager = FlattenDataType.getManager(data);
        try {
            flatten = new FlattenDataType(FlattenDataType.FlattenEntry.cityFlatten, indexes, data);
        } catch (IOException | URISyntaxException e) {
            throw new AssertionError(e);
        }
    }
    
    @BeforeClass
    public static void filterSetup() throws Exception {
        accumuloSetup.setData(FileType.JSON, flatten);
        connector = accumuloSetup.loadTables(log);
    }
    
    public SimpleFlattenQueryTest() {
        super(manager);
    }
    
    @Test
    public void testState() throws Exception {
        log.info("------  testState  ------");
        String state = "'teXas'";
        String query = SimpleField.STATE + EQ_OP + state;
        runTest(query, query);
    }
    
    @Test
    public void testMultiState() throws Exception {
        log.info("------  testMultiState  ------");
        String texas = "'teXas'";
        String oregon = "'oReGon'";
        String query = SimpleField.STATE + EQ_OP + texas + OR_OP + SimpleField.STATE.name() + EQ_OP + oregon;
        runTest(query, query);
    }
    
    @Test(expected = InvalidQueryException.class)
    public void testErrorDataDictionary() throws Exception {
        log.info("------  testErrorDataDictionary  ------");
        String city = "'salem'";
        String query = "CITY" + EQ_OP + city;
        runTest(query, query);
    }
    
    // end of unit tests
    // ============================================
    
    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = FlattenDataType.getTestAuths();
        this.documentKey = SimpleField.EVENTID.name();
    }
    
    private enum SimpleField {
        // include base fields - name must match
        // since enum cannot be extends - replicate these entries
        STARTDATE(FlattenBaseFields.STARTDATE.getNormalizer()),
        EVENTID(FlattenBaseFields.EVENTID.getNormalizer()),
        STATE(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
        
        static final List<String> headers;
        
        static {
            headers = new ArrayList<>();
            headers.addAll(Stream.of(SimpleField.values()).map(e -> e.name()).collect(Collectors.toList()));
        }
        
        static final Map<String,RawMetaData> metadataMapping = new HashMap<>();
        static {
            for (SimpleField field : SimpleField.values()) {
                RawMetaData data = new RawMetaData(field.name(), field.normalizer, false);
                metadataMapping.put(field.name().toLowerCase(), data);
            }
        }
        
        private final Normalizer<?> normalizer;
        
        SimpleField(Normalizer<?> norm) {
            this.normalizer = norm;
        }
    }
    
    private static class SimpleIndexing extends AbstractFields {
        
        private static final Collection<String> index = new HashSet<>();
        private static final Collection<String> indexOnly = new HashSet<>();
        private static final Collection<String> reverse = new HashSet<>();
        private static final Collection<String> multivalue = new HashSet<>();
        
        private static final Collection<Set<String>> composite = new HashSet<>();
        private static final Collection<Set<String>> virtual = new HashSet<>();
        
        static {
            // set index configuration values
            index.add(SimpleField.STATE.name());
            reverse.addAll(index);
        }
        
        SimpleIndexing() {
            super(index, indexOnly, reverse, multivalue, composite, virtual);
        }
        
        @Override
        public String toString() {
            return "SimpleIndexing{" + super.toString() + "}";
        }
    }
}
