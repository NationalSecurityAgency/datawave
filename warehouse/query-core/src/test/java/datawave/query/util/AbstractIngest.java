package datawave.query.util;

import static datawave.table.constants.TableName.METADATA;
import static datawave.table.constants.TableName.SHARD;
import static datawave.table.constants.TableName.SHARD_INDEX;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.Type;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.Uid;
import datawave.query.jexl.JexlASTHelper;
import datawave.table.hash.UID;
import datawave.test.MacTestUtil;
import datawave.util.time.DateHelper;

/**
 * Simple ingest utility for writing fields and values to the big three tables
 */
public class AbstractIngest {

    private static final Logger log = LoggerFactory.getLogger(AbstractIngest.class);

    private final AccumuloClient client;
    private final Authorizations auths;
    private final TableOperations tops;

    private final Multimap<String,String> fieldColumns = ArrayListMultimap.create();
    private final Map<String,Type<?>> normalizers = new HashMap<>();

    // default values
    private static final String DATE = "20260708";
    private static final String ROW = DATE + "_0";
    private static final String DATATYPE = "datatype-a";
    private static final Long TIMESTAMP = DateHelper.parse(DATE).getTime();
    private static final Value EMPTY_VALUE = new Value();

    public AbstractIngest(AccumuloClient client, Authorizations auths) throws AccumuloException, AccumuloSecurityException {
        this.client = client;
        this.auths = auths;
        this.tops = client.tableOperations();

        MacTestUtil.createOrRecreate(tops, METADATA);
        MacTestUtil.createOrRecreate(tops, SHARD_INDEX);
        MacTestUtil.createOrRecreate(tops, SHARD);

        SecurityOperations sops = client.securityOperations();
        sops.changeUserAuthorizations("root", auths);
    }

    /**
     * Registers a field and it's normalizer. For now fields can have at most one normalizer.
     *
     * @param field
     *            the field
     * @param normalizer
     *            the normalizer
     */
    public void registerField(String field, Type<?> normalizer) {
        try (BatchWriter bw = client.createBatchWriter(METADATA)) {
            normalizers.put(field, normalizer);
            Mutation m = new Mutation(field);
            m.put("t", DATATYPE + "\0" + normalizer.getClass().getName(), EMPTY_VALUE);
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerColumns(String field, List<String> columns) {
        try (BatchWriter bw = client.createBatchWriter(METADATA)) {
            Mutation m = new Mutation(field);
            for (String column : columns) {
                switch (column) {
                    case "i":
                    case "ri":
                    case "e":
                    case "tf":
                        fieldColumns.putAll(field, columns);
                        m.put(column, DATATYPE, EMPTY_VALUE);
                        break;
                    case "t":
                        // ignore
                        break;
                    default:
                        throw new RuntimeException("Unsupported metadata column: " + column);
                }
            }
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeFV(int eventId, String field, String value) {
        String uid = uid(eventId);
        String normalizedValue = getNormalizedValue(field, value);

        writeShardIndex(uid, field, normalizedValue);
        writeFieldIndex(uid, field, normalizedValue);
        writeEvent(uid, field, value);
    }

    public void writeTokenized(int eventId, String field, String phrase) {
        String uid = uid(eventId);
        String[] tokens = phrase.split(" ");
        String[] normalizedTokens = Arrays.stream(tokens).map(token -> getNormalizedValue(field, token)).toArray(String[]::new);
        String baseField = JexlASTHelper.deconstructIdentifier(field);

        writeShardIndex(uid, baseField, normalizedTokens);
        writeFieldIndex(uid, baseField, normalizedTokens);
        // persist context for the TF column
        writeTermFrequency(uid, field, normalizedTokens);
    }

    private void writeShardIndex(String uid, String field, String... values) {
        if (fieldColumns.containsEntry(field, "i")) {
            try (BatchWriter bw = client.createBatchWriter(SHARD_INDEX)) {
                for (String value : values) {
                    Mutation m = new Mutation(value);
                    Text cf = new Text(field);
                    Text cq = new Text(ROW + "\0" + DATATYPE);
                    ColumnVisibility cv = new ColumnVisibility(auths.iterator().next());
                    m.put(cf, cq, cv, TIMESTAMP, getValue(uid));
                    bw.addMutation(m);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void writeFieldIndex(String uid, String field, String... values) {
        if (fieldColumns.containsEntry(field, "i")) {
            try (BatchWriter bw = client.createBatchWriter(SHARD)) {
                Mutation m = new Mutation(ROW);
                Text cf = new Text("fi\0" + field);
                for (String value : values) {
                    Text cq = new Text(value + "\0" + DATATYPE + "\0" + uid);
                    ColumnVisibility cv = new ColumnVisibility(auths.iterator().next());
                    m.put(cf, cq, cv, TIMESTAMP, EMPTY_VALUE);
                }
                bw.addMutation(m);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void writeEvent(String uid, String field, String value) {
        if (fieldColumns.containsEntry(field, "e")) {
            try (BatchWriter bw = client.createBatchWriter(SHARD)) {
                Mutation m = new Mutation(ROW);
                Text cf = new Text(DATATYPE + "\0" + uid);
                Text cq = new Text(field + "\0" + value);
                ColumnVisibility cv = new ColumnVisibility(auths.iterator().next());
                m.put(cf, cq, cv, TIMESTAMP, EMPTY_VALUE);
                bw.addMutation(m);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void writeTermFrequency(String uid, String field, String... values) {
        String baseField = JexlASTHelper.deconstructIdentifier(field);
        if (fieldColumns.containsEntry(baseField, "i") && fieldColumns.containsEntry(baseField, "tf")) {
            try (BatchWriter bw = client.createBatchWriter(SHARD)) {
                Mutation m = new Mutation(ROW);
                Text cf = new Text("tf");
                for (int i = 0; i < values.length; i++) {
                    String value = values[i];
                    Text cq = new Text(DATATYPE + "\0" + uid + "\0" + value + "\0" + field);
                    ColumnVisibility cv = new ColumnVisibility(auths.iterator().next());

                    TermWeight.Info info = TermWeight.Info.newBuilder().addTermOffset(i).build();
                    Value termFrequencyValue = new Value(info.toByteArray());
                    m.put(cf, cq, cv, TIMESTAMP, termFrequencyValue);
                }
                bw.addMutation(m);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Generate a {@link datawave.table.hash.HashUID} for the given integer event id
     *
     * @param id
     *            the event id
     * @return the uid for the event
     */
    public String uid(int id) {
        return UID.builder().newId(String.valueOf(id).getBytes(), (Date) null).toString();
    }

    /**
     * Normalize the value using a preregistered {@link Type}.
     *
     * @param field
     *            the field
     * @param value
     *            the value
     * @return the normalized value
     */
    private String getNormalizedValue(String field, String value) {
        String baseField = JexlASTHelper.deconstructIdentifier(field);
        Type<?> normalizer = normalizers.get(baseField);
        if (normalizer == null) {
            throw new RuntimeException("Did not find normalizer registered for field " + baseField);
        }

        return normalizer.normalize(value);
    }

    private Value getValue(String uid) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(false);
        builder.setCOUNT(1L);
        builder.addUID(uid);
        return new Value(builder.build().toByteArray());
    }

    /**
     * Debugging utility that prints an entire table
     *
     * @param name
     *            the table name
     */
    public void printTable(String name) {
        try (Scanner scanner = client.createScanner(name, auths)) {
            log.info("=== {} ===", name);
            for (Map.Entry<Key,Value> entry : scanner) {
                log.info("k: {} v: {}", entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Debugging utility that prints an entire table, filtered to keys that contain the provided 'target' parameter
     * <p>
     * For example, prints all keys that match an event's uid
     *
     * @param name
     *            the table name
     * @param target
     *            the filter target
     */
    public void printTable(String name, String target) {
        try (Scanner scanner = client.createScanner(name, auths)) {
            log.info("=== {} ===", name);
            log.info("filter: {}", target);
            for (Map.Entry<Key,Value> entry : scanner) {
                String key = entry.getKey().toString();
                if (name.equals(SHARD_INDEX)) {
                    Uid.List docIds = Uid.List.parseFrom(entry.getValue().get());
                    List<String> uids = docIds.getUIDList();
                    if (uids.contains(target)) {
                        log.info("k: {} v: {}", key, entry.getValue());
                    }
                } else if (key.contains(target)) {
                    log.info("k: {} v: {}", key, entry.getValue());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getDate() {
        return DATE;
    }
}
