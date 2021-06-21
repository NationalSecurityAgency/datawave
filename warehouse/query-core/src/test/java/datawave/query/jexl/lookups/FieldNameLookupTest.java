package datawave.query.jexl.lookups;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static datawave.query.jexl.visitors.FixUnfieldedTermsVisitorTest.buildMutation;
import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.Assert.assertEquals;

public class FieldNameLookupTest {
    
    // Controls how much data is generated
    private static int years = 1;
    private static int shardsPerDay = 17;
    
    private static AccumuloClient client;
    private static ScannerFactory scannerFactory;
    private static ShardQueryConfiguration config;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        client = new InMemoryAccumuloClient("", new InMemoryInstance());
        client.tableOperations().create(SHARD_INDEX);
        
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        BatchWriter bw = client.createBatchWriter(SHARD_INDEX, bwConfig);
        
        // Load data according to years and shardsPerDay
        List<String> docIds = Arrays.asList("uid0", "uid1", "uid2", "uid3");
        
        int startYear = 2000;
        LocalDate start = LocalDate.of(startYear, 1, 1);
        LocalDate end = LocalDate.of(startYear + years, 1, 1);
        
        DateTimeFormatter formatter = DateTimeFormatter.BASIC_ISO_DATE;
        String date;
        String shard;
        while (start.compareTo(end) < 0) {
            date = start.format(formatter);
            for (int ii = 1; ii <= shardsPerDay; ii++) {
                shard = date + "_" + ii;
                bw.addMutation(buildMutation("AAA", "bar", shard, "datatype", docIds));
                bw.addMutation(buildMutation("BBB", "bar", shard, "datatype", docIds));
                bw.addMutation(buildMutation("CCC", "bar", shard, "datatype", docIds));
                bw.addMutation(buildMutation("DDD", "bar", shard, "datatype", docIds));
                bw.addMutation(buildMutation("EEE", "bar", shard, "datatype", docIds));
                bw.addMutation(buildMutation("FOO", "bar", shard, "datatype", docIds));
            }
            start = start.plus(1L, ChronoUnit.DAYS);
        }
        
        // Add one more mutation with a different field name at the very end.
        bw.addMutation(buildMutation("FOO2", "bar", start.format(formatter) + "_" + 1, "datatype", docIds));
        
        bw.flush();
        bw.close();
    }
    
    // fresh config for each test.
    @Before
    public void before() {
        // Setup ShardQueryConfiguration
        config = new ShardQueryConfiguration();
        config.setDatatypeFilter(Collections.singleton("datatype"));
        
        // Set begin/end date for query
        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));
        
        // Build and set datatypes
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Collections.singleton(new LcNoDiacriticsType()));
        dataTypes.putAll("FOO2", Collections.singleton(new LcNoDiacriticsType()));
        dataTypes.putAll("AAA", Collections.singleton(new LcNoDiacriticsType()));
        dataTypes.putAll("BBB", Collections.singleton(new LcNoDiacriticsType()));
        dataTypes.putAll("CCC", Collections.singleton(new LcNoDiacriticsType()));
        dataTypes.putAll("DDD", Collections.singleton(new LcNoDiacriticsType()));
        dataTypes.putAll("EEE", Collections.singleton(new LcNoDiacriticsType()));
        
        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);
        
        config.setClient(client);
        
        scannerFactory = new ScannerFactory(config.getClient(), 1);
    }
    
    // Set the begin/end dates in yyyyMMdd.
    public void setBeginEndDates(String begin, String end) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            config.setBeginDate(sdf.parse(begin));
            config.setEndDate(sdf.parse(end));
        } catch (java.text.ParseException e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testLookupFullDateRange() {
        Set<String> fields = Sets.newHashSet("FOO", "FOO2");
        Set<String> terms = Collections.singleton("bar");
        FieldNameLookup lookup = new FieldNameLookup(fields, terms);
        
        IndexLookupMap lookupMap = lookup.lookup(config, scannerFactory, 10L);
        
        assertEquals("{FOO=[bar], FOO2=[bar]}", lookupMap.toString());
    }
    
    // Query should reduce the fields based on date range.
    @Test
    public void testLookupOneMonth() {
        Set<String> fields = Sets.newHashSet("FOO", "FOO2");
        Set<String> terms = Collections.singleton("bar");
        FieldNameLookup lookup = new FieldNameLookup(fields, terms);
        
        setBeginEndDates("20000101", "20000131");
        
        IndexLookupMap lookupMap = lookup.lookup(config, scannerFactory, 10L);
        
        assertEquals("{FOO=[bar]}", lookupMap.toString());
    }
}
