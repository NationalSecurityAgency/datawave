package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MockMetadataHelper;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
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
import java.util.concurrent.TimeUnit;

import static datawave.util.TableName.METADATA;
import static datawave.util.TableName.SHARD_INDEX;
import static datawave.util.TableName.SHARD_RINDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * fluffy bunnies, cats, doggos, oh my!
 */
public class FixUnfieldedTermsVisitorTest {
    
    // Control how much data is generated.
    private static int months = 3;
    private static int shardsPerDay = 17;
    
    private static AccumuloClient client;
    private static ScannerFactory scannerFactory;
    private static ShardQueryConfiguration config;
    private static MockMetadataHelper metadataHelper;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        client = new InMemoryAccumuloClient("", new InMemoryInstance());
        client.tableOperations().create(SHARD_INDEX);
        client.tableOperations().create(SHARD_RINDEX);
        client.tableOperations().create(METADATA);
        
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        BatchWriter bw = client.createBatchWriter(SHARD_INDEX, bwConfig);
        
        // Load data according to years and shardsPerDay
        List<String> docIds = Arrays.asList("uid0", "uid1", "uid2", "uid3");
        
        LocalDate start = LocalDate.of(2020, 1, 1);
        LocalDate end = LocalDate.of(2020, 1 + months, 1);
        
        DateTimeFormatter formatter = DateTimeFormatter.BASIC_ISO_DATE;
        String date;
        String shard;
        while (start.compareTo(end) < 0) {
            date = start.format(formatter);
            for (int ii = 1; ii <= shardsPerDay; ii++) {
                shard = date + "_" + ii;
                
                switch (start.getMonthValue()) {
                    case 1:
                        bw.addMutation(buildMutation("BUNNY", "fluffy", shard, "datatype", docIds));
                        break;
                    case 2:
                        bw.addMutation(buildMutation("CAT", "fluffy", shard, "datatype", docIds));
                        break;
                    case 3:
                        bw.addMutation(buildMutation("DOG", "fluffy", shard, "datatype", docIds));
                        break;
                    default:
                        break;
                }
            }
            start = start.plus(1L, ChronoUnit.DAYS);
        }
        
        bw.flush();
        bw.close();
        
        metadataHelper = new MockMetadataHelper();
        metadataHelper.setIndexedFields(Sets.newHashSet("BUNNY", "CAT", "DOG"));
    }
    
    // Fresh config for each test.
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
        dataTypes.putAll("BUNNY", Collections.singleton(new LcNoDiacriticsType()));
        dataTypes.putAll("CAT", Collections.singleton(new LcNoDiacriticsType()));
        dataTypes.putAll("DOG", Collections.singleton(new LcNoDiacriticsType()));
        
        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);
        
        config.setClient(client);
        
        scannerFactory = new ScannerFactory(config.getClient());
    }
    
    public static Mutation buildMutation(String fieldName, String fieldValue, String shard, String datatype, List<String> docIds) {
        Text columnFamily = new Text(fieldName);
        Text columnQualifier = new Text(shard + '\u0000' + datatype);
        
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setIGNORE(false);
        builder.setCOUNT(docIds.size());
        Uid.List list = builder.build();
        
        Value value = new Value(list.toByteArray());
        
        Mutation mutation = new Mutation(fieldValue);
        mutation.put(columnFamily, columnQualifier, value);
        return mutation;
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
    
    // Should expand to all three fields
    @Test
    public void testExpansionFullRange() throws ParseException {
        String query = "_ANYFIELD_ == 'fluffy'";
        String expected = "(CAT == 'fluffy' || DOG == 'fluffy' || BUNNY == 'fluffy')";
        setBeginEndDates("20200101", "20200331");
        assertExpected(query, expected);
    }
    
    // Should expand to single term
    @Test
    public void testExpansionAcrossFirstMonth() throws ParseException {
        String query = "_ANYFIELD_ == 'fluffy'";
        String expected = "BUNNY == 'fluffy'";
        setBeginEndDates("20200101", "20200131");
        assertExpected(query, expected);
    }
    
    // Should expand to two terms
    @Test
    public void testExpansionAcrossTwoMonths() throws ParseException {
        String query = "_ANYFIELD_ == 'fluffy'";
        String expected = "(CAT == 'fluffy' || BUNNY == 'fluffy')";
        setBeginEndDates("20200101", "20200228");
        assertExpected(query, expected);
    }
    
    // Should expand to different single term
    @Test
    public void testExpansionAcrossLastMonth() throws ParseException {
        String query = "_ANYFIELD_ == 'fluffy'";
        String expected = "DOG == 'fluffy'";
        setBeginEndDates("20200301", "20200331");
        assertExpected(query, expected);
    }
    
    @Test
    public void testMaxValueExpansionThreshold() throws ParseException {
        String query = "_ANYFIELD_ == 'fluffy'";
        String expected = "(CAT == 'fluffy' || DOG == 'fluffy' || BUNNY == 'fluffy')";
        setBeginEndDates("20200101", "20200331");
        assertExpected(query, expected);
        
        // MaxValueExpansionThreshold should not affect this query.
        config.setMaxValueExpansionThreshold(2);
        assertExpected(query, expected);
    }
    
    @Test
    public void testMaxUnfieldedExpansionThreshold() throws ParseException {
        String query = "_ANYFIELD_ == 'fluffy'";
        String expected = "(CAT == 'fluffy' || DOG == 'fluffy' || BUNNY == 'fluffy')";
        setBeginEndDates("20200101", "20200331");
        assertExpected(query, expected);
        
        // MaxValueExpansionThreshold should be respected.
        config.setMaxUnfieldedExpansionThreshold(2);
        expected = "((_Term_ = true) && (_ANYFIELD_ == 'fluffy'))";
        assertExpected(query, expected);
    }
    
    public void assertExpected(String query, String expected) throws ParseException {
        
        ASTJexlScript parsedScript = JexlASTHelper.parseJexlQuery(query);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        
        try {
            ASTJexlScript fixedScript = FixUnfieldedTermsVisitor.fixUnfieldedTree(config, scannerFactory, metadataHelper, parsedScript, true, true, false);
            
            String expectedString = JexlStringBuildingVisitor.buildQueryWithoutParse(expectedScript);
            String fixedString = JexlStringBuildingVisitor.buildQueryWithoutParse(fixedScript);
            assertEquals(expectedString, fixedString);
            
        } catch (InstantiationException | IllegalAccessException | TableNotFoundException e) {
            e.printStackTrace();
            fail("Error running FixUnfieldedTermsVisitor" + e.getMessage());
        }
    }
}
