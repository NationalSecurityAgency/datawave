package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MockMetadataHelper;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static datawave.util.TableName.METADATA;
import static datawave.util.TableName.SHARD_INDEX;
import static datawave.util.TableName.SHARD_RINDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RangeConjunctionRebuildingVisitorTest {
    
    // Control how much data is generated.
    private static int months = 7;
    private static int shardsPerDay = 5;
    
    private static Connector connector;
    private static ScannerFactory scannerFactory;
    private static ShardQueryConfiguration config;
    private static MockMetadataHelper metadataHelper;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        connector = instance.getConnector("", new PasswordToken(new byte[0]));
        connector.tableOperations().create(SHARD_INDEX);
        connector.tableOperations().create(SHARD_RINDEX);
        connector.tableOperations().create(METADATA);
        
        scannerFactory = new ScannerFactory(connector);
        
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1024L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        BatchWriter bw = connector.createBatchWriter(SHARD_INDEX, bwConfig);
        
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
                        bw.addMutation(buildMutation("FOO", "bad", shard, "datatype", docIds));
                        break;
                    case 2:
                        bw.addMutation(buildMutation("FOO", "bar", shard, "datatype", docIds));
                        break;
                    case 3:
                        bw.addMutation(buildMutation("FOO", "barz", shard, "datatype", docIds));
                        break;
                    case 4:
                        bw.addMutation(buildMutation("FOO", "bard", shard, "datatype", docIds));
                        break;
                    case 5:
                        bw.addMutation(buildMutation("FOO", "barge", shard, "datatype", docIds));
                        break;
                    case 6:
                        bw.addMutation(buildMutation("FOO", "barstool", shard, "datatype", docIds));
                        break;
                    case 7:
                        bw.addMutation(buildMutation("FOO", "baz", shard, "datatype", docIds));
                        break;
                    default:
                        break;
                }
            }
            start = start.plus(1L, ChronoUnit.DAYS);
        }
        
        bw.flush();
        bw.close();
        
        // Setup ShardQueryConfiguration
        config = new ShardQueryConfiguration();
        config.setDatatypeFilter(Collections.singleton("datatype"));
        
        // Set begin/end date for query
        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));
        
        // Build and set datatypes
        Multimap<String,Type<?>> dataTypes = HashMultimap.create();
        dataTypes.putAll("FOO", Collections.singleton(new LcNoDiacriticsType()));
        
        config.setQueryFieldsDatatypes(dataTypes);
        config.setIndexedFields(dataTypes);
        
        metadataHelper = new MockMetadataHelper();
        metadataHelper.setIndexedFields(Collections.singleton("FOO"));
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
    
    // Should expand to all terms that start with 'bar'
    @Test
    public void testRangeQueryAgainstFullDateRange() throws ParseException {
        String query = "FOO >= 'bar' && FOO <= 'bas'";
        String expected = "(FOO == 'bar' || FOO == 'barge' || FOO == 'barz' || FOO == 'barstool' || FOO == 'bard')";
        setBeginEndDates("20200101", "20200731");
        assertExpected(query, expected);
    }
    
    // Should expand query to three discreet terms found within the date range
    @Test
    public void testRangeQueryAgainstSmallDateRange() throws ParseException {
        String query = "FOO >= 'bar' && FOO <= 'bas'";
        String expected = "(FOO == 'bar' || FOO == 'barz' || FOO == 'bard')";
        setBeginEndDates("20200201", "20200430");
        assertExpected(query, expected);
    }
    
    // If the index lookup has no results the original query structure should be preserved.
    @Test
    public void testRangeAgainstInvalidDateRange() throws ParseException {
        String query = "FOO >= 'bar' && FOO <= 'bas'";
        String expected = "FOO >= 'bar' && FOO <= 'bas'";
        setBeginEndDates("20000101", "20000102");
        assertExpected(query, expected);
    }
    
    public void assertExpected(String query, String expected) throws ParseException {
        
        ASTJexlScript parsedScript = JexlASTHelper.parseJexlQuery(query);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        
        try {
            ASTJexlScript expandedScript = RangeConjunctionRebuildingVisitor.expandRanges(config, scannerFactory, metadataHelper, parsedScript, true, true);
            
            String expectedString = JexlStringBuildingVisitor.buildQueryWithoutParse(expectedScript);
            String expandedString = JexlStringBuildingVisitor.buildQueryWithoutParse(expandedScript);
            assertEquals(expectedString, expandedString);
            
        } catch (TableNotFoundException | ExecutionException e) {
            e.printStackTrace();
            fail("Error running RangeConjunctionRebuildingVisitor" + e.getMessage());
        }
    }
}
