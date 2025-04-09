package datawave.iterators;

import static datawave.data.ColumnFamilyConstants.COLF_DESC;
import static datawave.data.ColumnFamilyConstants.COLF_E;
import static datawave.data.ColumnFamilyConstants.COLF_F;
import static datawave.data.ColumnFamilyConstants.COLF_I;
import static datawave.data.ColumnFamilyConstants.COLF_N;
import static datawave.data.ColumnFamilyConstants.COLF_RI;
import static datawave.query.util.TestUtils.createDateFrequencyMap;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.model.DateFrequencyMap;
import datawave.query.util.TestUtils;
import datawave.security.util.ScannerHelper;

public class FrequencyMetadataAggregatorTest {
    
    private static final String TABLE_METADATA = "metadata";
    private static final String[] AUTHS = {"FOO", "BAR", "COB"};
    private static final Set<Authorizations> AUTHS_SET = Collections.singleton(new Authorizations(AUTHS));
    private static final String NULL_BYTE = "\0";
    
    private AccumuloClient accumuloClient;
    private Boolean combineColumnVisibilities;
    private final List<Map.Entry<Key,Value>> expected = new ArrayList<>();
    private final List<Mutation> mutations = new ArrayList<>();
    
    @BeforeAll
    static void beforeAll() throws URISyntaxException {
        File dir = new File(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(".")).toURI());
        File targetDir = dir.getParentFile();
        System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());
    }
    
    @BeforeEach
    public void setUp() throws Exception {
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(FrequencyMetadataAggregatorTest.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_METADATA)) {
            accumuloClient.tableOperations().create(TABLE_METADATA);
        }
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        accumuloClient.tableOperations().deleteRows(TABLE_METADATA, null, null);
        combineColumnVisibilities = null;
        expected.clear();
    }
    
    /**
     * Verify that aggregation of entries for the columns "f", "i", and "ri" in their non-aggregated format (e.g. when they're initially ingested) are
     * aggregated correctly.
     */
    @Test
    void testDifferingColumnFamilies() throws TableNotFoundException {
        // "f" rows.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000004L, "20200102", 2L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200103", 3L);
        
        // "i" rows.
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000000L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000001L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000002L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000001L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000002L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000003L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000004L, "20200102", 2L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000000L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000002L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_I, "csv", "FOO", 1500000003L, "20200103", 3L);
        
        // "ri" rows.
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000000L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000001L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000002L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000003L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000004L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000000L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000001L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000002L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000003L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000004L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000005L, "20200102", 2L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000000L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000001L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000002L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000003L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1500000004L, "20200103", 3L);
        
        expect("NAME", COLF_F, "csv" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 4L, "20200102", 10L, "20200103", 12L));
        expect("NAME", COLF_I, "csv" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 3L, "20200102", 8L, "20200103", 9L));
        expect("NAME", COLF_RI, "csv" + NULL_BYTE + "AGGREGATED", "FOO", 1500000005L, createDateFrequencyMap("20200101", 5L, "20200102", 12L, "20200103", 15L));
        
        assertResults();
    }
    
    /**
     * Verify that entries with the same name, column family, and column visibility are separated by their datatype.
     */
    @Test
    void testDifferingDatatypes() throws TableNotFoundException {
        // Datatype "csv".
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000004L, "20200102", 2L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200103", 3L);
        
        // Datatype "wiki".
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000000L, "20200101", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000001L, "20200101", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000002L, "20200101", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000003L, "20200101", 3L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000000L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000001L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000002L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000003L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000000L, "20200103", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000001L, "20200103", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000002L, "20200103", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1500000003L, "20200103", 2L);
        
        // Datatype "text".
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000000L, "20200102", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000001L, "20200102", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000002L, "20200102", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000015L, "20200102", 3L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000000L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000001L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000002L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000003L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000000L, "20200104", 4L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000001L, "20200104", 4L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000002L, "20200104", 4L);
        givenNonAggregatedRow("NAME", COLF_F, "text", "FOO", 1500000003L, "20200104", 4L);
        
        expect("NAME", COLF_F, "csv" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 4L, "20200102", 10L, "20200103", 12L));
        expect("NAME", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "FOO", 1500000015L, createDateFrequencyMap("20200102", 12L, "20200103", 4L, "20200104", 16L));
        expect("NAME", COLF_F, "wiki" + NULL_BYTE + "AGGREGATED", "FOO", 1500000003L, createDateFrequencyMap("20200101", 12L, "20200102", 4L, "20200103", 8L));
        
        assertResults();
    }
    
    /**
     * Verify that when entries for the same field, column family, datatype, and date are aggregated, that the aggregated entries are still separated by their
     * column visibility by default.
     */
    @Test
    public void testDifferingColumnVisibilities() throws TableNotFoundException {
        // Column visibility "FOO".
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000004L, "20200102", 2L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200103", 3L);
        
        // Column visibility "BAR".
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000000L, "20200101", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000001L, "20200101", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000002L, "20200101", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000003L, "20200101", 3L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000000L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000001L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000002L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000003L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000000L, "20200103", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000001L, "20200103", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000002L, "20200103", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000003L, "20200103", 2L);
        
        // Column visibility "COB".
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000000L, "20200102", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000001L, "20200102", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000002L, "20200102", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000015L, "20200102", 3L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000000L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000001L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000002L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000003L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000000L, "20200104", 4L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000001L, "20200104", 4L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000002L, "20200104", 4L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000003L, "20200104", 4L);
        
        expect("NAME", COLF_F, "csv" + NULL_BYTE + "AGGREGATED", "BAR", 1500000003L, createDateFrequencyMap("20200101", 12L, "20200102", 4L, "20200103", 8L));
        expect("NAME", COLF_F, "csv" + NULL_BYTE + "AGGREGATED", "COB", 1500000015L, createDateFrequencyMap("20200102", 12L, "20200103", 4L, "20200104", 16L));
        expect("NAME", COLF_F, "csv" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 4L, "20200102", 10L, "20200103", 12L));
        
        assertResults();
    }
    
    /**
     * Verify that when the iterator option {@link FrequencyMetadataAggregator#COMBINE_VISIBILITIES_OPTION} is set to true, entries with same field, column
     * family, datatype, and date are aggregated and their column visibilities are combined.
     */
    @Test
    public void testCombiningColumnVisibilities() throws TableNotFoundException {
        // Column visibility "FOO".
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000004L, "20200102", 2L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200103", 3L);
        
        // Column visibility "BAR".
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000000L, "20200101", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000001L, "20200101", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000002L, "20200101", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000003L, "20200101", 3L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000000L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000001L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000002L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000003L, "20200102", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000000L, "20200103", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000001L, "20200103", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000002L, "20200103", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "BAR", 1500000003L, "20200103", 2L);
        
        // Column visibility "COB".
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000000L, "20200102", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000001L, "20200102", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000002L, "20200102", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000015L, "20200102", 3L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000000L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000001L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000002L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000003L, "20200103", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000000L, "20200104", 4L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000001L, "20200104", 4L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000002L, "20200104", 4L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "COB", 1500000003L, "20200104", 4L);
        
        // Enable to option to combine visibilities.
        givenCombineColumnVisibilitiesIsTrue();
        
        expect("NAME", COLF_F, "csv" + NULL_BYTE + "AGGREGATED", "BAR&COB&FOO", 1500000015L,
                        createDateFrequencyMap("20200101", 16L, "20200102", 26L, "20200103", 24L, "20200104", 16L));
        
        assertResults();
    }
    
    /**
     * Verify that aggregating non-aggregated entries into a previously-aggregated row works correctly.
     */
    @Test
    void testAggregatedAndNonAggregatedEntries() throws TableNotFoundException {
        // Aggregated entry.
        givenAggregatedRow("NAME", COLF_F, "csv", "FOO", 1499999999L, createDateFrequencyMap("20191225", 40L, "20200101", 15L, "20200102", 20L));
        
        // Non-aggregated entry.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200101", 1L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200102", 2L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000004L, "20200102", 2L); // Latest timestamp.
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000000L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000001L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000002L, "20200103", 3L);
        givenNonAggregatedRow("NAME", COLF_F, "csv", "FOO", 1500000003L, "20200103", 3L);
        
        expect("NAME", COLF_F, "csv" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L,
                        createDateFrequencyMap("20191225", 40L, "20200101", 19L, "20200102", 30L, "20200103", 12L));
        
        assertResults();
    }
    
    /**
     * Verify that entries not requiring any aggregation are not modified.
     */
    @Test
    void testNoAggregationNeeded() throws TableNotFoundException {
        givenAggregatedRow("NAME", COLF_F, "csv", "FOO", 1499999995L, createDateFrequencyMap("20191225", 40L, "20200101", 15L, "20200102", 20L));
        givenAggregatedRow("NAME", COLF_I, "csv", "FOO", 1499999995L, createDateFrequencyMap("20191225", 40L, "20200101", 15L, "20200102", 20L));
        givenAggregatedRow("NAME", COLF_RI, "csv", "FOO", 1499999995L, createDateFrequencyMap("20191225", 40L, "20200101", 15L, "20200102", 20L));
        givenAggregatedRow("NAME", COLF_F, "text", "FOO", 1499999995L, createDateFrequencyMap("20200101", 20L, "20200102", 10L));
        givenAggregatedRow("NAME", COLF_I, "text", "FOO", 1499999995L, createDateFrequencyMap("20200101", 20L, "20200102", 10L));
        givenAggregatedRow("NAME", COLF_RI, "text", "FOO", 1499999995L, createDateFrequencyMap("20200101", 20L, "20200102", 10L));
        givenAggregatedRow("NAME", COLF_F, "wiki", "FOO", 1499999995L, createDateFrequencyMap("20191225", 20L, "20200101", 10L));
        givenAggregatedRow("NAME", COLF_I, "wiki", "FOO", 1499999995L, createDateFrequencyMap("20191225", 20L, "20200101", 10L));
        givenAggregatedRow("NAME", COLF_RI, "wiki", "FOO", 1499999995L, createDateFrequencyMap("20191225", 20L, "20200101", 10L));
        givenAggregatedRow("GENDER", COLF_F, "attr", "BAR", 1499999995L, createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        givenAggregatedRow("GENDER", COLF_I, "attr", "BAR", 1499999995L, createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        givenAggregatedRow("GENDER", COLF_RI, "attr", "BAR", 1499999995L, createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        givenAggregatedRow("GENDER", COLF_F, "attr", "FOO", 1499999995L, createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        givenAggregatedRow("GENDER", COLF_I, "attr", "FOO", 1499999995L, createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        givenAggregatedRow("GENDER", COLF_RI, "attr", "FOO", 1499999995L, createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        
        expect("GENDER", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999995L,
                        createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        expect("GENDER", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L,
                        createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        expect("GENDER", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999995L,
                        createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        expect("GENDER", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L,
                        createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        expect("GENDER", COLF_RI, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999995L,
                        createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        expect("GENDER", COLF_RI, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L,
                        createDateFrequencyMap("20191220", 20L, "20191225", 10L, "20191230", 11L));
        expect("NAME", COLF_F, "csv" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L, createDateFrequencyMap("20191225", 40L, "20200101", 15L, "20200102", 20L));
        expect("NAME", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L, createDateFrequencyMap("20200101", 20L, "20200102", 10L));
        expect("NAME", COLF_F, "wiki" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L, createDateFrequencyMap("20191225", 20L, "20200101", 10L));
        expect("NAME", COLF_I, "csv" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L, createDateFrequencyMap("20191225", 40L, "20200101", 15L, "20200102", 20L));
        expect("NAME", COLF_I, "text" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L, createDateFrequencyMap("20200101", 20L, "20200102", 10L));
        expect("NAME", COLF_I, "wiki" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L, createDateFrequencyMap("20191225", 20L, "20200101", 10L));
        expect("NAME", COLF_RI, "csv" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L,
                        createDateFrequencyMap("20191225", 40L, "20200101", 15L, "20200102", 20L));
        expect("NAME", COLF_RI, "text" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L, createDateFrequencyMap("20200101", 20L, "20200102", 10L));
        expect("NAME", COLF_RI, "wiki" + NULL_BYTE + "AGGREGATED", "FOO", 1499999995L, createDateFrequencyMap("20191225", 20L, "20200101", 10L));
        
        assertResults();
    }
    
    /**
     * Test aggregation over a more diverse dataset of mixed aggregated and non-aggregated rows.
     */
    @Test
    void testDiverseDataset() throws TableNotFoundException {
        givenAggregatedRow("AGE", COLF_F, "num", "FOO", 1499999995L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1499999995L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_I, "num", "FOO", 1499999999L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_I, "lifetime", "FOO", 1499999999L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("GENDER", COLF_F, "text", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        givenAggregatedRow("NAME", COLF_F, "attr", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        givenAggregatedRow("NAME", COLF_I, "attr", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        
        givenNonAggregatedRow("AGE", COLF_F, "num", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_I, "num", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_I, "lifetime", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_F, "var", "BAR", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new datatype.
        givenNonAggregatedRow("GENDER", COLF_F, "text", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new colvis.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_I, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        
        expect("AGE", COLF_F, "lifetime" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L,
                        createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_F, "num" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_F, "var" + NULL_BYTE + "AGGREGATED", "BAR", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("AGE", COLF_I, "lifetime" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L,
                        createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_I, "num" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("GENDER", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        expect("GENDER", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("JOB", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 3L));
        expect("JOB", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("NAME", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        expect("NAME", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        
        assertResults();
    }
    
    /**
     * Test aggregation over a dataset that contains index markers.
     */
    @Test
    void testIndexMarkers() throws TableNotFoundException {
        givenAggregatedRow("AGE", COLF_F, "num", "FOO", 1499999995L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1499999995L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_I, "num", "FOO", 1499999999L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_I, "lifetime", "FOO", 1499999999L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("GENDER", COLF_F, "text", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        givenAggregatedRow("NAME", COLF_F, "attr", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        givenAggregatedRow("NAME", COLF_I, "attr", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        
        givenNonAggregatedRow("AGE", COLF_F, "num", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_I, "num", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_I, "lifetime", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_F, "var", "BAR", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new datatype.
        givenNonAggregatedRow("GENDER", COLF_F, "text", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new colvis.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_I, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        
        // Entries with index markers that should not be aggregated.
        givenMutation("AGE", COLF_I, "num" + NULL_BYTE + "20191230" + NULL_BYTE + "true", "BAR", 1400000005L, new Value());
        givenMutation("JOB", COLF_RI, "attr" + NULL_BYTE + "20190530" + NULL_BYTE + "false", "FOO", 1500000004L, new Value());
        givenMutation("NAME", COLF_I, "attr" + NULL_BYTE + "20171201" + NULL_BYTE + "true", "BAR", 1500000004L, new Value());
        
        expect("AGE", COLF_F, "lifetime" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L,
                        createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_F, "num" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_F, "var" + NULL_BYTE + "AGGREGATED", "BAR", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("AGE", COLF_I, "lifetime" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L,
                        createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_I, "num" + NULL_BYTE + "20191230" + NULL_BYTE + "true", "BAR", 1400000005L, new Value());
        expect("AGE", COLF_I, "num" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("GENDER", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        expect("GENDER", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("JOB", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 3L));
        expect("JOB", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("JOB", COLF_RI, "attr" + NULL_BYTE + "20190530" + NULL_BYTE + "false", "FOO", 1500000004L, new Value());
        expect("NAME", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        expect("NAME", COLF_I, "attr" + NULL_BYTE + "20171201" + NULL_BYTE + "true", "BAR", 1500000004L, new Value());
        expect("NAME", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        
        assertResults();
    }
    
    /**
     * Test aggregation over a dataset that contains legacy formats.
     */
    @Test
    void testLegacyFormats() throws TableNotFoundException {
        givenAggregatedRow("AGE", COLF_F, "num", "FOO", 1499999995L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1499999995L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_I, "num", "FOO", 1499999999L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_I, "lifetime", "FOO", 1499999999L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("GENDER", COLF_F, "text", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        givenAggregatedRow("NAME", COLF_F, "attr", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        givenAggregatedRow("NAME", COLF_I, "attr", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        
        givenNonAggregatedRow("AGE", COLF_F, "num", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_I, "num", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_I, "lifetime", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_F, "var", "BAR", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new datatype.
        givenNonAggregatedRow("GENDER", COLF_F, "text", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new colvis.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_I, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        
        // Entries with legacy formats that should not be aggregated.
        givenMutation("AGE", COLF_I, "num", "BAR", 1400000005L, new Value());
        givenMutation("JOB", COLF_RI, "attr" + NULL_BYTE + "FakeTypeClassName", "FOO", 1500000004L, new Value());
        
        expect("AGE", COLF_F, "lifetime" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L,
                        createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_F, "num" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_F, "var" + NULL_BYTE + "AGGREGATED", "BAR", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("AGE", COLF_I, "lifetime" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L,
                        createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_I, "num", "BAR", 1400000005L, new Value());
        expect("AGE", COLF_I, "num" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("GENDER", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        expect("GENDER", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("JOB", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 3L));
        expect("JOB", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("JOB", COLF_RI, "attr" + NULL_BYTE + "FakeTypeClassName", "FOO", 1500000004L, new Value());
        expect("NAME", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        expect("NAME", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        
        assertResults();
    }
    
    /**
     * Verify that scanning over a table with columns that are not to be aggregated result in them being unchanged.
     */
    @Test
    void testMixedColumns() throws TableNotFoundException {
        givenAggregatedRow("AGE", COLF_F, "num", "FOO", 1499999995L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1499999995L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_I, "num", "FOO", 1499999999L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("AGE", COLF_I, "lifetime", "FOO", 1499999999L, createDateFrequencyMap("20191225", 1L, "20200101", 1L, "20200102", 1L));
        givenAggregatedRow("GENDER", COLF_F, "text", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        givenAggregatedRow("NAME", COLF_F, "attr", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        givenAggregatedRow("NAME", COLF_I, "attr", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        
        givenNonAggregatedRow("AGE", COLF_F, "num", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_I, "num", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_I, "lifetime", "FOO", 1500000004L, "20200101", 1L); // Should be aggregated into existing aggregated entry.
        givenNonAggregatedRow("AGE", COLF_F, "var", "BAR", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new datatype.
        givenNonAggregatedRow("GENDER", COLF_F, "text", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new colvis.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        givenNonAggregatedRow("JOB", COLF_I, "attr", "FOO", 1500000004L, "20200101", 1L); // Should result in new aggregated entry because new row.
        
        // Non frequency rows that should not be affected by aggregation.
        givenMutation("AGE", COLF_E, "num", "BAR", 1400000005L, new Value());
        givenMutation("AGE", COLF_DESC, "num", "BAR", 1400000005L, new Value("age_num description"));
        givenMutation("AGE", COLF_E, "lifetime", "BAR", 1400000005L, new Value());
        givenMutation("AGE", COLF_DESC, "lifetime", "BAR", 1400000005L, new Value("age_lifetime description"));
        givenMutation("AGE", COLF_DESC, "var", "BAR", 1400000005L, new Value("age_var description"));
        givenMutation("JOB", COLF_E, "attr", "BAR", 1400000005L, new Value());
        givenMutation("JOB", COLF_DESC, "attr", "BAR", 1400000005L, new Value("job_attr description"));
        givenMutation("GENDER", COLF_DESC, "text", "BAR", 1400000005L, new Value("gender_text description"));
        givenMutation("JOB", new Text("m"), "attr", "BAR", 1400000005L, new Value());
        
        expect("AGE", COLF_DESC, "lifetime", "BAR", 1400000005L, new Value("age_lifetime description"));
        expect("AGE", COLF_DESC, "num", "BAR", 1400000005L, new Value("age_num description"));
        expect("AGE", COLF_DESC, "var", "BAR", 1400000005L, new Value("age_var description"));
        expect("AGE", COLF_E, "lifetime", "BAR", 1400000005L, new Value());
        expect("AGE", COLF_E, "num", "BAR", 1400000005L, new Value());
        expect("AGE", COLF_F, "lifetime" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L,
                        createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_F, "num" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_F, "var" + NULL_BYTE + "AGGREGATED", "BAR", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("AGE", COLF_I, "lifetime" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L,
                        createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("AGE", COLF_I, "num" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20191225", 1L, "20200101", 2L, "20200102", 1L));
        expect("GENDER", COLF_DESC, "text", "BAR", 1400000005L, new Value("gender_text description"));
        expect("GENDER", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        expect("GENDER", COLF_F, "text" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("JOB", COLF_DESC, "attr", "BAR", 1400000005L, new Value("job_attr description"));
        expect("JOB", COLF_E, "attr", "BAR", 1400000005L, new Value());
        expect("JOB", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 3L));
        expect("JOB", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "FOO", 1500000004L, createDateFrequencyMap("20200101", 1L));
        expect("JOB", new Text("m"), "attr", "BAR", 1400000005L, new Value());
        expect("NAME", COLF_F, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        expect("NAME", COLF_I, "attr" + NULL_BYTE + "AGGREGATED", "BAR", 1499999999L, createDateFrequencyMap("20200101", 1L, "20200102", 1L));
        
        assertResults();
    }
    
    private void assertResults() throws TableNotFoundException {
        TestUtils.writeMutations(accumuloClient, TABLE_METADATA, mutations);
        Scanner scanner = createScanner();
        List<Map.Entry<Key,Value>> actual = new ArrayList<>();
        for (Map.Entry<Key,Value> entry : scanner) {
            actual.add(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
            System.out.println("Key: " + entry.getKey());
        }
        
        Assertions.assertEquals(expected, actual);
    }
    
    private Scanner createScanner() throws TableNotFoundException {
        Scanner scanner = ScannerHelper.createScanner(accumuloClient, TABLE_METADATA, AUTHS_SET);
        scanner.setRange(new Range());
        
        IteratorSetting iteratorSetting = new IteratorSetting(10, FrequencyMetadataAggregator.class);
        if (combineColumnVisibilities != null) {
            iteratorSetting.addOption(FrequencyMetadataAggregator.COMBINE_VISIBILITIES_OPTION, String.valueOf(combineColumnVisibilities));
        }
        iteratorSetting.addOption(FrequencyMetadataAggregator.COLUMNS_OPTION, "f,i,ri");
        scanner.addScanIterator(iteratorSetting);
        
        return scanner;
    }
    
    private void givenCombineColumnVisibilitiesIsTrue() {
        this.combineColumnVisibilities = true;
    }
    
    private void givenNonAggregatedRow(String row, Text colf, String datatype, String colv, long timestamp, String date, long count) {
        givenMutation(row, colf, datatype + NULL_BYTE + date, colv, timestamp, new Value(LongCombiner.VAR_LEN_ENCODER.encode(count)));
    }
    
    private void givenAggregatedRow(String row, Text colf, String datatype, String colv, long timestamp, DateFrequencyMap map) {
        givenMutation(row, colf, datatype + NULL_BYTE + FrequencyMetadataAggregator.AGGREGATED, colv, timestamp, new Value(WritableUtils.toByteArray(map)));
    }
    
    private void givenMutation(String row, Text colf, String colq, String colv, long timestamp, Value value) {
        Mutation mutation = new Mutation(row);
        mutation.put(colf, new Text(colq), new ColumnVisibility(colv), timestamp, value);
        this.mutations.add(mutation);
    }
    
    private void expect(String row, Text colf, String colq, String colv, long timestamp, DateFrequencyMap map) {
        expect(row, colf, colq, colv, timestamp, new Value(WritableUtils.toByteArray(map)));
    }
    
    private void expect(String row, Text colf, String colq, String colv, long timestamp, Value value) {
        expected.add(new AbstractMap.SimpleEntry<>(new Key(new Text(row), colf, new Text(colq), new ColumnVisibility(colv), timestamp), value));
    }
}
