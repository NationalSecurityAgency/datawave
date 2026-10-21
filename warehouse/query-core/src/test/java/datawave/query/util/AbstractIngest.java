package datawave.query.util;

import static datawave.table.constants.TableName.METADATA;
import static datawave.table.constants.TableName.SHARD;
import static datawave.table.constants.TableName.SHARD_INDEX;
import static datawave.table.constants.TableName.SHARD_RINDEX;

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
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.data.ColumnFamilyConstants;
import datawave.data.type.Type;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.Uid;
import datawave.query.jexl.JexlASTHelper;
import datawave.table.hash.UID;
import datawave.test.MacTestUtil;
import datawave.util.time.DateHelper;

/**
 * Simple ingest utility for writing fields and values to the big three tables
 * <ul>
 * <li>{@link datawave.table.constants.TableName#METADATA}</li>
 * <li>{@link datawave.table.constants.TableName#SHARD_INDEX}</li>
 * <li>{@link datawave.table.constants.TableName#SHARD}</li>
 * </ul>
 * The simplest use of this class relies on default values for the row and datatype. Methods are provided for operators who require unique rows and datatypes,
 * but this information must be tracked separately.
 * <p>
 * <b>Multiple datatypes:</b> {@link #registerColumns(String, String, List)} and {@link #writeFV(String, String, int, String, String)} accept an explicit
 * datatype, so a field's shard/event/index rows can be written per-datatype. However the column gating tracked by {@link #fieldColumns} is keyed by field name
 * only, not by (field, datatype) - so a field cannot be indexed for one datatype and unindexed for another via {@link #registerColumns}. To express a field
 * index hole that differs by datatype, index the field the same way for every datatype via {@code registerColumns}, then use
 * {@link #writeMetadataCounts(String, String, String, long, long)} to write the per-datatype, per-day frequency/index counts that
 * {@code AllFieldMetadataHelper#getFieldIndexHoles} actually reads.
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
        MacTestUtil.createOrRecreate(tops, SHARD_RINDEX);
        MacTestUtil.createOrRecreate(tops, SHARD);

        SecurityOperations sops = client.securityOperations();
        sops.changeUserAuthorizations("root", auths);
    }

    /**
     * Registers a field and it's normalizer for the default datatype. For now fields can have at most one normalizer.
     *
     * @param field
     *            the field
     * @param normalizer
     *            the normalizer
     */
    public void registerField(String field, Type<?> normalizer) {
        registerField(field, DATATYPE, normalizer);
    }

    /**
     * Registers a field and it's normalizer for the given datatype. For now fields can have at most one normalizer.
     *
     * @param field
     *            the field
     * @param datatype
     *            the datatype
     * @param normalizer
     *            the normalizer
     */
    public void registerField(String field, String datatype, Type<?> normalizer) {
        try (BatchWriter bw = client.createBatchWriter(METADATA)) {
            normalizers.put(field, normalizer);
            Mutation m = new Mutation(field);
            m.put("t", datatype + "\0" + normalizer.getClass().getName(), EMPTY_VALUE);
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerColumns(String field, List<String> columns) {
        registerColumns(field, DATATYPE, columns);
    }

    public void registerColumns(String field, String datatype, List<String> columns) {
        try (BatchWriter bw = client.createBatchWriter(METADATA)) {
            Mutation m = new Mutation(field);
            for (String column : columns) {
                switch (column) {
                    case "i":
                    case "ri":
                    case "e":
                    case "tf":
                    case "content":
                        fieldColumns.putAll(field, columns);
                        m.put(column, datatype, EMPTY_VALUE);
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

    /**
     * Mark the given columns as write-gated for a field (enabling {@link #writeFV}/{@link #writeTokenized} to write the corresponding shard/index rows) without
     * writing {@link #registerColumns}'s boolean METADATA declaration row (a {@code cf=<column>, cq=<datatype>} row with no date).
     * <p>
     * Use this instead of {@link #registerColumns} for the {@code i}/{@code ri} columns of a field driven by {@link #writeMetadataCounts}: that declaration row
     * is indistinguishable from a dateless index boundary marker to {@code AllFieldMetadataHelper.FieldIndexHoleFinder}, and since it is written with
     * Accumulo's current-time default timestamp, it gets misread as an index boundary at "yesterday" (relative to ingest time) - corrupting hole detection for
     * any query date range that doesn't happen to fall after that point.
     *
     * @param field
     *            the field name
     * @param columns
     *            the write-gating columns, e.g. {@code List.of("i", "e")}
     */
    public void registerWriteGatingColumns(String field, List<String> columns) {
        for (String column : columns) {
            switch (column) {
                case "i":
                case "ri":
                case "e":
                case "tf":
                case "content":
                case "t":
                    break;
                default:
                    throw new RuntimeException("Unsupported metadata column: " + column);
            }
        }
        fieldColumns.putAll(field, columns);
    }

    /**
     * Write the frequency and index day-counts for a field, datatype, and date that {@code AllFieldMetadataHelper#getFieldIndexHoles} reads to determine field
     * index holes, using the default datatype. A day is a field index hole when a frequency count is written but the index count is absent, or the ratio of
     * index count to frequency count falls below the configured minimum threshold.
     *
     * @param field
     *            the field name
     * @param date
     *            the date, in {@code yyyyMMdd} format
     * @param freqCount
     *            the frequency count
     * @param indexCount
     *            the index count
     */
    public void writeMetadataCounts(String field, String date, long freqCount, long indexCount) {
        writeMetadataCounts(field, DATATYPE, date, freqCount, indexCount);
    }

    /**
     * Write the frequency and index day-counts for a field, datatype, and date that {@code AllFieldMetadataHelper#getFieldIndexHoles} reads to determine field
     * index holes. A day is a field index hole when a frequency count is written but the index count is absent, or the ratio of index count to frequency count
     * falls below the configured minimum threshold.
     *
     * @param field
     *            the field name
     * @param datatype
     *            the datatype
     * @param date
     *            the date, in {@code yyyyMMdd} format
     * @param freqCount
     *            the frequency count
     * @param indexCount
     *            the index count. Note the index/reverse-index count row is always written, even when {@code 0} - the underlying hole finder treats a missing
     *            index entry as an unconditional hole rather than running it through the threshold comparison, which would otherwise make it impossible to
     *            express a {@code 0} frequency count with a matching {@code 0} index count (i.e. a day with no data at all) as "not a hole".
     */
    public void writeMetadataCounts(String field, String datatype, String date, long freqCount, long indexCount) {
        try (BatchWriter bw = client.createBatchWriter(METADATA)) {
            Mutation m = new Mutation(field);
            Text cq = new Text(datatype + "\0" + date);
            m.put(ColumnFamilyConstants.COLF_F, cq, new Value(SummingCombiner.VAR_LEN_ENCODER.encode(freqCount)));
            m.put(ColumnFamilyConstants.COLF_I, cq, new Value(SummingCombiner.VAR_LEN_ENCODER.encode(indexCount)));
            m.put(ColumnFamilyConstants.COLF_RI, cq, new Value(SummingCombiner.VAR_LEN_ENCODER.encode(indexCount)));
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Write the provided event id, field and value for the default row and datatype
     *
     * @param eventId
     *            the event id
     * @param field
     *            the field name
     * @param value
     *            the field value
     */
    public void writeFV(int eventId, String field, String value) {
        writeFV(ROW, DATATYPE, eventId, field, value);
    }

    /**
     * Write the provided datatype, event id, field and value for the default row
     *
     * @param datatype
     *            the datatype
     * @param eventId
     *            the event id
     * @param field
     *            the field name
     * @param value
     *            the field value
     */
    public void writeFV(String datatype, int eventId, String field, String value) {
        writeFV(ROW, datatype, eventId, field, value);
    }

    /**
     * Write the provided row, datatype, event id, field and value
     *
     * @param row
     *            the row
     * @param datatype
     *            the datatype
     * @param eventId
     *            the event id
     * @param field
     *            the field name
     * @param value
     *            the field value
     */
    public void writeFV(String row, String datatype, int eventId, String field, String value) {
        writeFVForUid(row, datatype, uid(eventId), field, value);
    }

    /**
     * Write the provided row, datatype, uid, field and value.
     * <p>
     * Unlike {@link #writeFV(String, String, int, String, String)} the uid is supplied directly rather than derived from an event id. This is what allows a
     * test to express a top level document structure, where a child's uid is its parent's uid suffixed with {@code .N}. Operators writing a child must track
     * the parent's uid themselves, via {@link #uid(int)}.
     *
     * @param row
     *            the row
     * @param datatype
     *            the datatype
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param value
     *            the field value
     */
    public void writeFVForUid(String row, String datatype, String uid, String field, String value) {
        String normalizedValue = getNormalizedValue(field, value);

        writeShardIndex(row, datatype, uid, field, List.of(normalizedValue));
        writeFieldIndex(row, datatype, uid, field, List.of(normalizedValue));
        writeEvent(row, datatype, uid, field, value);
    }

    /**
     * Write a tokens for a space-delimited phrase for the specified event id, and field using the default row and datatype
     *
     * @param eventId
     *            the event id
     * @param field
     *            the field name
     * @param phrase
     *            the phrase
     */
    public void writeTokenized(int eventId, String field, String phrase) {
        writeTokenized(ROW, DATATYPE, eventId, field, phrase);
    }

    /**
     * Write a tokens for a space-delimited phrase for the specified datatype, event id, and field using the default row
     *
     * @param datatype
     *            the datatype
     * @param eventId
     *            the event id
     * @param field
     *            the field name
     * @param phrase
     *            the phrase
     */
    public void writeTokenized(String datatype, int eventId, String field, String phrase) {
        writeTokenized(ROW, datatype, eventId, field, phrase);
    }

    /**
     * Write a tokens for a space-delimited phrase for the specified row, datatype, event id, and field
     *
     * @param row
     *            the row
     * @param datatype
     *            the datatype
     * @param eventId
     *            the event id
     * @param field
     *            the field name
     * @param phrase
     *            the phrase
     */
    public void writeTokenized(String row, String datatype, int eventId, String field, String phrase) {
        String uid = uid(eventId);
        String[] tokens = phrase.split(" ");
        String[] normalizedTokens = Arrays.stream(tokens).map(token -> getNormalizedValue(field, token)).toArray(String[]::new);
        String baseField = JexlASTHelper.deconstructIdentifier(field);

        writeShardIndex(row, datatype, uid, baseField, List.of(normalizedTokens));
        writeFieldIndex(row, datatype, uid, baseField, List.of(normalizedTokens));
        // persist context for the TF column
        writeTermFrequency(uid, field, normalizedTokens);
    }

    /**
     * Write a field and values for the provided uid, using the default row and datatype
     *
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param values
     *            the field values
     */
    private void writeShardIndex(String uid, String field, String... values) {
        writeShardIndex(ROW, DATATYPE, uid, field, List.of(values));
    }

    /**
     * Write a field and values for the provided datatype and uid, using the default row
     *
     * @param datatype
     *            the datatype
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param values
     *            the field values
     */
    private void writeShardIndex(String datatype, String uid, String field, List<String> values) {
        writeShardIndex(ROW, datatype, uid, field, values);
    }

    /**
     * Write a field and values for the provided row, datatype and uid
     *
     * @param row
     *            the row
     * @param datatype
     *            the datatype
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param values
     *            the field values
     */
    private void writeShardIndex(String row, String datatype, String uid, String field, List<String> values) {
        if (fieldColumns.containsEntry(field, "i")) {
            long timestamp = timestampForRow(row);
            try (BatchWriter bw = client.createBatchWriter(SHARD_INDEX)) {
                for (String value : values) {
                    Mutation m = new Mutation(value);
                    Text cf = new Text(field);
                    Text cq = new Text(row + "\0" + datatype);
                    ColumnVisibility cv = new ColumnVisibility(auths.iterator().next());
                    m.put(cf, cq, cv, timestamp, getIndexUidValue(uid));
                    bw.addMutation(m);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Write a field and values for the provided uid using the default row and datatype
     *
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param values
     *            the field values
     */
    private void writeFieldIndex(String uid, String field, String... values) {
        writeFieldIndex(ROW, DATATYPE, uid, field, List.of(values));
    }

    /**
     * Write a field and values for the provided datatype and uid using the default row
     *
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param values
     *            the field values
     */
    private void writeFieldIndex(String datatype, String uid, String field, List<String> values) {
        writeFieldIndex(ROW, datatype, uid, field, values);
    }

    /**
     * Write a field and values for the provided row, datatype and uid
     *
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param values
     *            the field values
     */
    private void writeFieldIndex(String row, String datatype, String uid, String field, List<String> values) {
        if (fieldColumns.containsEntry(field, "i")) {
            long timestamp = timestampForRow(row);
            try (BatchWriter bw = client.createBatchWriter(SHARD)) {
                Mutation m = new Mutation(row);
                Text cf = new Text("fi\0" + field);
                for (String value : values) {
                    Text cq = new Text(value + "\0" + datatype + "\0" + uid);
                    ColumnVisibility cv = new ColumnVisibility(auths.iterator().next());
                    m.put(cf, cq, cv, timestamp, EMPTY_VALUE);
                }
                bw.addMutation(m);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Write a field and value for a specific uid using the default row and datatype.
     *
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param value
     *            the field value
     */
    private void writeEvent(String uid, String field, String value) {
        writeEvent(ROW, DATATYPE, uid, field, value);
    }

    /**
     * Write a field and value for a specific datatype and uid using the default row
     *
     * @param datatype
     *            the datatype
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param value
     *            the field value
     */
    private void writeEvent(String datatype, String uid, String field, String value) {
        writeEvent(ROW, datatype, uid, field, value);
    }

    /**
     * Write a field and value for the provided row, datatype and uid
     *
     * @param row
     *            the row
     * @param datatype
     *            the datatype
     * @param uid
     *            the event uid
     * @param field
     *            the field name
     * @param value
     *            the field value
     */
    private void writeEvent(String row, String datatype, String uid, String field, String value) {
        if (fieldColumns.containsEntry(field, "e")) {
            try (BatchWriter bw = client.createBatchWriter(SHARD)) {
                Mutation m = new Mutation(row);
                Text cf = new Text(datatype + "\0" + uid);
                Text cq = new Text(field + "\0" + value);
                ColumnVisibility cv = new ColumnVisibility(auths.iterator().next());
                m.put(cf, cq, cv, timestampForRow(row), EMPTY_VALUE);
                bw.addMutation(m);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Derive a timestamp from a row's {@code yyyyMMdd} date prefix (rows are formatted as {@code yyyyMMdd_N}), so that events written for different dates get
     * timestamps matching their date rather than the fixed default {@link #TIMESTAMP}.
     *
     * @param row
     *            the row, e.g. {@code 20260701_0}
     * @return the timestamp for the row's date
     */
    private long timestampForRow(String row) {
        String date = row.contains("_") ? row.substring(0, row.indexOf('_')) : row;
        return DateHelper.parse(date).getTime();
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

    /**
     * Create a {@link Value} for the global index that contains a single uid
     * <p>
     * <b>NOTE:</b> if uid aggregation is desired the operator must configure the correct table aggregator
     *
     * @param uid
     *            the uid
     * @return a serialized uid
     */
    private Value getIndexUidValue(String uid) {
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
        if (!tops.exists(name)) {
            throw new RuntimeException("Table " + name + " does not exist");
        }

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
        if (!tops.exists(name)) {
            throw new RuntimeException("Table " + name + " does not exist");
        }

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

    /**
     * Get the default date
     *
     * @return the date
     */
    public String getDate() {
        return DATE;
    }

    /**
     * Get the default row
     *
     * @return the row
     */
    public String getRow() {
        return ROW;
    }

    /**
     * Get the default datatype
     *
     * @return the datatype
     */
    public String getDatatype() {
        return DATATYPE;
    }

    /**
     * Get the default timestamp
     *
     * @return
     */
    public long getTimestamp() {
        return TIMESTAMP;
    }
}
