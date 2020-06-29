package datawave.iterators.filter;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.security.util.ScannerHelper;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class EntryRegexFilterTest {
    
    private static final String TABLE_NAME = "testTable";
    private static final String[] AUTH_ARRAY = new String[] {"PRIVATE", "PUBLIC"};
    private static final Set<Authorizations> AUTHS = Collections.singleton(new Authorizations(AUTH_ARRAY));
    
    private static final Entry ENTRY_ONE = new Entry("bar_field", "text", "NumberType", "PUBLIC", "MD5");
    private static final Entry ENTRY_TWO = new Entry("edge_vertex", "csv", "LcdType", "PRIVATE", "LoadDate");
    private static final Entry ENTRY_THREE = new Entry("foo_field", "rar", "NumberType", "PUBLIC", "Name");
    
    private Connector connector;
    
    @Before
    public void setUp() throws Exception {
        connector = new InMemoryInstance("test").getConnector("root", new PasswordToken(""));
        connector.tableOperations().create(TABLE_NAME);
        BatchWriter writer = connector.createBatchWriter(TABLE_NAME, new BatchWriterConfig());
        writer.addMutation(ENTRY_ONE.toMutation());
        writer.addMutation(ENTRY_TWO.toMutation());
        writer.addMutation(ENTRY_THREE.toMutation());
        writer.close();
    }
    
    @After
    public void tearDown() throws Exception {
        if (connector != null && connector.tableOperations().exists(TABLE_NAME)) {
            connector.tableOperations().delete(TABLE_NAME);
        }
    }
    
    @Test
    public void testOptionsConfigurator() {
        IteratorSetting setting = new IteratorSetting(1, "test", EntryRegexFilter.class.getName());
        
        // Test the individual field configuration methods.
        EntryRegexFilter.configureOptions(setting).rowRegex("row").columnFamilyRegex("cf").columnQualifierRegex("cq").visibilityRegex("visibility")
                        .valueRegex("value").caseInsensitive().matchSubstrings().orMatches().encoding("UTF-16");
        
        Map<String,String> options = setting.getOptions();
        assertThat(options.get(EntryRegexFilter.ROW_REGEX)).isEqualTo("row");
        assertThat(options.get(EntryRegexFilter.COLUMN_FAMILY_REGEX)).isEqualTo("cf");
        assertThat(options.get(EntryRegexFilter.COLUMN_QUALIFIER_REGEX)).isEqualTo("cq");
        assertThat(options.get(EntryRegexFilter.VISIBILITY_REGEX)).isEqualTo("visibility");
        assertThat(options.get(EntryRegexFilter.VALUE_REGEX)).isEqualTo("value");
        assertThat(options.get(EntryRegexFilter.CASE_INSENSITIVE)).isEqualTo("true");
        assertThat(options.get(EntryRegexFilter.MATCH_SUBSTRINGS)).isEqualTo("true");
        assertThat(options.get(EntryRegexFilter.OR_MATCHES)).isEqualTo("true");
        assertThat(options.get(EntryRegexFilter.ENCODING)).isEqualTo("UTF-16");
        
        // Test setting a regex for all fields.
        EntryRegexFilter.configureOptions(setting).allRegex("all");
        assertThat(options.get(EntryRegexFilter.ROW_REGEX)).isEqualTo("all");
        assertThat(options.get(EntryRegexFilter.COLUMN_FAMILY_REGEX)).isEqualTo("all");
        assertThat(options.get(EntryRegexFilter.COLUMN_QUALIFIER_REGEX)).isEqualTo("all");
        assertThat(options.get(EntryRegexFilter.VISIBILITY_REGEX)).isEqualTo("all");
        assertThat(options.get(EntryRegexFilter.VALUE_REGEX)).isEqualTo("all");
    }
    
    @Test
    public void testRowMatch() throws TableNotFoundException {
        IteratorSetting filter = new IteratorSetting(1, "regexFilter", EntryRegexFilter.class.getName());
        EntryRegexFilter.configureOptions(filter).rowRegex("bar_field");
        
        List<Entry> entries = scanEntries(filter);
        assertThat(entries).containsExactly(ENTRY_ONE);
    }
    
    @Test
    public void testColumnFamilyMatch() throws TableNotFoundException {
        IteratorSetting filter = new IteratorSetting(1, "regexFilter", EntryRegexFilter.class.getName());
        EntryRegexFilter.configureOptions(filter).columnFamilyRegex("csv");
        
        List<Entry> entries = scanEntries(filter);
        assertThat(entries).containsExactly(ENTRY_TWO);
    }
    
    @Test
    public void testColumnQualifierMatch() throws TableNotFoundException {
        IteratorSetting filter = new IteratorSetting(1, "regexFilter", EntryRegexFilter.class.getName());
        EntryRegexFilter.configureOptions(filter).columnQualifierRegex("LcdType");
        
        List<Entry> entries = scanEntries(filter);
        assertThat(entries).containsExactly(ENTRY_TWO);
    }
    
    @Test
    public void testColumnVisibilityMatch() throws TableNotFoundException {
        IteratorSetting filter = new IteratorSetting(1, "regexFilter", EntryRegexFilter.class.getName());
        EntryRegexFilter.configureOptions(filter).visibilityRegex("PUBLIC");
        
        List<Entry> entries = scanEntries(filter);
        assertThat(entries).containsExactly(ENTRY_ONE, ENTRY_THREE);
    }
    
    @Test
    public void testValueMatch() throws TableNotFoundException {
        IteratorSetting filter = new IteratorSetting(1, "regexFilter", EntryRegexFilter.class.getName());
        EntryRegexFilter.configureOptions(filter).valueRegex("Name");
        
        List<Entry> entries = scanEntries(filter);
        assertThat(entries).containsExactly(ENTRY_THREE);
    }
    
    @Test
    public void testCaseInsensitivity() throws TableNotFoundException {
        IteratorSetting filter = new IteratorSetting(1, "regexFilter", EntryRegexFilter.class.getName());
        EntryRegexFilter.configureOptions(filter).columnQualifierRegex("numbertype").caseInsensitive();
        
        List<Entry> entries = scanEntries(filter);
        assertThat(entries).containsExactly(ENTRY_ONE, ENTRY_THREE);
    }
    
    @Test
    public void testMatchSubstrings() throws TableNotFoundException {
        IteratorSetting filter = new IteratorSetting(1, "regexFilter", EntryRegexFilter.class.getName());
        EntryRegexFilter.configureOptions(filter).rowRegex("field").matchSubstrings();
        
        List<Entry> entries = scanEntries(filter);
        assertThat(entries).containsExactly(ENTRY_ONE, ENTRY_THREE);
    }
    
    @Test
    public void testOrMatches() throws TableNotFoundException {
        IteratorSetting filter = new IteratorSetting(1, "regexFilter", EntryRegexFilter.class.getName());
        EntryRegexFilter.configureOptions(filter).allRegex("bar_field").orMatches();
        
        List<Entry> entries = scanEntries(filter);
        assertThat(entries).containsExactly(ENTRY_ONE);
    }
    
    private List<Entry> scanEntries(IteratorSetting regexFilter) throws TableNotFoundException {
        Scanner scanner = ScannerHelper.createScanner(connector, TABLE_NAME, AUTHS);
        scanner.addScanIterator(regexFilter);
        
        List<Entry> entries = new ArrayList<>();
        for (Map.Entry<Key,Value> entry : scanner) {
            entries.add(new Entry(entry));
        }
        return entries;
    }
    
    private static class Entry {
        
        private final Text row;
        private final Text columnFamily;
        private final Text columnQualifier;
        private final Text columnVisibility;
        private final Value value;
        
        Entry(String row, String colf, String colq, String visibility, String value) {
            this.row = new Text(row);
            this.columnFamily = new Text(colf);
            this.columnQualifier = new Text(colq);
            this.columnVisibility = new Text(visibility);
            this.value = new Value(value);
        }
        
        Entry(Map.Entry<Key,Value> entry) {
            this.row = entry.getKey().getRow();
            this.columnFamily = entry.getKey().getColumnFamily();
            this.columnQualifier = entry.getKey().getColumnQualifier();
            this.columnVisibility = entry.getKey().getColumnVisibility();
            this.value = entry.getValue();
        }
        
        Mutation toMutation() {
            Mutation mutation = new Mutation(row);
            mutation.put(columnFamily, columnQualifier, new ColumnVisibility(columnVisibility), value);
            return mutation;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Entry entry = (Entry) o;
            return Objects.equals(row, entry.row) && Objects.equals(columnFamily, entry.columnFamily) && Objects.equals(columnQualifier, entry.columnQualifier)
                            && Objects.equals(columnVisibility, entry.columnVisibility) && Objects.equals(value, entry.value);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(row, columnFamily, columnQualifier, columnVisibility, value);
        }
        
        @Override
        public String toString() {
            return new ToStringBuilder(this).append("row", row).append("columnFamily", columnFamily).append("columnQualifier", columnQualifier)
                            .append("columnVisibility", columnVisibility).append("value", value).toString();
        }
    }
}
