package datawave.test.framework.writers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeEach;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.TableCreator;
import datawave.test.framework.util.MetadataColumn;

/**
 * Base class for the {@link TableWriter} tests.
 * <p>
 * The writers are asserted by round-tripping through an in-memory Accumulo instance rather than by mocking the {@code BatchWriter}, so the assertions are made
 * against the keys a scanner actually returns.
 * <p>
 * The tables are built by {@link TableCreator}, so they carry exactly the configuration a consumer of the framework gets - including the uid aggregator the
 * shard index depends on. That matters for {@link ShardIndexTableWriterTest}: the writer emits one single-uid entry per event and relies entirely on the
 * aggregator to merge the entries that collide within a shard, so a test scanning a plainly-created table would assert the wrong thing.
 */
public abstract class AbstractTableWriterTest {

    protected static final Authorizations AUTHS = new Authorizations(TableWriter.DEFAULT_VISIBILITY);
    protected static final int NUM_SHARDS = 10;
    protected static final String DATATYPE = "datatype-a";

    protected AccumuloClient client;

    @BeforeEach
    void createClientAndTables() throws Exception {
        // a fresh instance per test method, so entries written by one test cannot be scanned by another
        InMemoryInstance instance = new InMemoryInstance(getClass().getSimpleName() + "-" + UUID.randomUUID());
        client = new InMemoryAccumuloClient("root", instance);
        TableCreator.createTables(client);
        client.securityOperations().changeUserAuthorizations("root", AUTHS);
    }

    /**
     * Scan an entire table.
     *
     * @param table
     *            the table name
     * @return every entry in the table, in key order
     */
    protected List<Map.Entry<Key,Value>> scan(String table) {
        List<Map.Entry<Key,Value>> entries = new ArrayList<>();
        try (Scanner scanner = client.createScanner(table, AUTHS)) {
            for (Map.Entry<Key,Value> entry : scanner) {
                entries.add(entry);
            }
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
        return entries;
    }

    /**
     * Render a table's keys as {@code row cf:cq} strings, which keeps the assertions readable and independent of timestamp and visibility (both asserted
     * separately).
     *
     * @param table
     *            the table name
     * @return one string per entry, in key order
     */
    protected List<String> scanKeys(String table) {
        List<String> keys = new ArrayList<>();
        for (Map.Entry<Key,Value> entry : scan(table)) {
            Key key = entry.getKey();
            keys.add(key.getRow() + " " + key.getColumnFamily() + ":" + key.getColumnQualifier());
        }
        return keys;
    }

    /**
     * Build a field carrying the given metadata columns, a single datatype and the {@link LcNoDiacriticsType} normalizer.
     * <p>
     * Note that setting a normalizer implicitly adds {@link MetadataColumn#T}, so callers asserting exact metadata output must account for the type column.
     *
     * @param fieldName
     *            the field name
     * @param columns
     *            the metadata columns
     * @return the field metadata
     */
    protected FieldMetadata createField(String fieldName, MetadataColumn... columns) {
        FieldMetadata field = new FieldMetadata(fieldName);
        field.setMetadataColumns(List.of(columns));
        field.setDatatypes(List.of(DATATYPE));
        field.setNormalizers(List.of(new LcNoDiacriticsType()));
        return field;
    }

    /**
     * Build a field with explicit values and event ids, which is how every writer test pins the exact keys it expects.
     *
     * @param fieldName
     *            the field name
     * @param values
     *            the field's values
     * @param eventIds
     *            the events the field appears on
     * @param columns
     *            the metadata columns
     * @return the field metadata
     */
    protected FieldMetadata createPopulatedField(String fieldName, List<String> values, List<Integer> eventIds, MetadataColumn... columns) {
        FieldMetadata field = createField(fieldName, columns);
        field.setEventIds(eventIds);
        field.setValues(values);
        return field;
    }

    protected List<Type<?>> normalizers(Type<?>... types) {
        return List.of(types);
    }

    protected List<Integer> eventIdsInSameShard(int count) {
        List<Integer> eventIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            eventIds.add(1 + i * NUM_SHARDS);
        }
        return eventIds;
    }
}
