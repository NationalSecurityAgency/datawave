package datawave.query.util;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.composite.CompositeMetadataHelper;

public class MetadataHelperTest {
    
    private static final String TABLE_METADATA = "testMetadataTable";
    private static final String[] AUTHS = {"FOO"};
    private AccumuloClient accumuloClient;
    private MetadataHelper helper;
    
    @BeforeAll
    static void beforeAll() throws URISyntaxException {
        File dir = new File(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(".")).toURI());
        File targetDir = dir.getParentFile();
        System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());
    }
    
    @BeforeEach
    public void setup() throws TableNotFoundException, AccumuloException, TableExistsException, AccumuloSecurityException {
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(MetadataHelperTest.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_METADATA)) {
            accumuloClient.tableOperations().create(TABLE_METADATA);
        }
        helper = new MetadataHelper(createAllFieldMetadataHelper(), Collections.emptySet(), accumuloClient, TABLE_METADATA, Collections.emptySet(),
                        Collections.emptySet());
    }
    
    private AllFieldMetadataHelper createAllFieldMetadataHelper() {
        final Set<Authorizations> allMetadataAuths = Collections.emptySet();
        final Set<Authorizations> auths = Collections.singleton(new Authorizations(AUTHS));
        TypeMetadataHelper tmh = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, accumuloClient, TABLE_METADATA, auths, false);
        CompositeMetadataHelper cmh = new CompositeMetadataHelper(accumuloClient, TABLE_METADATA, auths);
        return new AllFieldMetadataHelper(tmh, cmh, accumuloClient, TABLE_METADATA, auths, allMetadataAuths);
    }
    
    @AfterEach
    void tearDown() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        accumuloClient.tableOperations().delete(TABLE_METADATA);
    }
    
    @Test
    public void testSingleFieldFilter() throws TableNotFoundException {
        writeMutation("rowA", "t", "dataTypeA", new Value("value"));
        
        Assertions.assertEquals(Collections.singleton("rowA"), helper.getAllFields(Collections.singleton("dataTypeA")));
        Assertions.assertEquals(Collections.singleton("rowA"), helper.getAllFields(null));
        Assertions.assertEquals(Collections.singleton("rowA"), helper.getAllFields(Collections.emptySet()));
    }
    
    @Test
    public void testMultipleFieldFilter() throws TableNotFoundException {
        writeMutation("rowA", "t", "dataTypeA", new Value("value"));
        writeMutation("rowB", "t", "dataTypeB", new Value("value"));
        
        Assertions.assertEquals(Collections.singleton("rowB"), helper.getAllFields(Collections.singleton("dataTypeB")));
        Assertions.assertEquals(Sets.newHashSet("rowA", "rowB"), helper.getAllFields(null));
        Assertions.assertEquals(Sets.newHashSet("rowA", "rowB"), helper.getAllFields(Collections.emptySet()));
    }
    
    @Test
    public void testMultipleFieldFilter2() throws TableNotFoundException {
        writeMutation("rowA", "t", "dataTypeA", new Value("value"));
        writeMutation("rowB", "t", "dataTypeB", new Value("value"));
        writeMutation("rowC", "t", "dataTypeC", new Value("value"));
        
        Assertions.assertEquals(Collections.singleton("rowB"), helper.getAllFields(Collections.singleton("dataTypeB")));
        Assertions.assertEquals(Sets.newHashSet("rowA", "rowB", "rowC"), helper.getAllFields(null));
        Assertions.assertEquals(Sets.newHashSet("rowA", "rowB", "rowC"), helper.getAllFields(Collections.emptySet()));
    }
    
    private void writeMutation(String row, String columnFamily, String columnQualifier, Value value) throws TableNotFoundException {
        Mutation mutation = new Mutation(row);
        mutation.put(columnFamily, columnQualifier, value);
        writeMutation(mutation);
    }
    
    private void writeMutation(Mutation m) throws TableNotFoundException {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = accumuloClient.createBatchWriter(TABLE_METADATA, config)) {
            writer.addMutation(m);
            writer.flush();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }
}
