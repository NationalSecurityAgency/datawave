package datawave.helpers;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.edge.util.ExtendedHyperLogLogPlus;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.Uid;
import datawave.iterators.FrequencyMetadataAggregator;
import datawave.metadata.protobuf.EdgeMetadata;
import datawave.query.model.DateFrequencyMap;
import datawave.query.table.parser.ContentKeyValueFactory;
import datawave.util.CompositeTimestamp;

/**
 * A set of static methods for printing tables in mock Accumulo instance. Additional {@link TableWriter} constants should be added as needed for tables that
 * have special timestamp or value formatting needs that cannot be satisfied by {@link #SIMPLE_TABLE_WRITER}.
 */
public final class PrintUtility {

    private static final Logger log = Logger.getLogger(PrintUtility.class);

    private static final DateTimeFormatter ISO_DATE_TIME_MILLIS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneId.systemDefault());

    /**
     * The default string representation of a {@link Value} that could not be decoded to a specific type.
     */
    private static final String UNDECODED_VALUE = "(unable to decode value)";

    private static final String NULL_BYTE = "\0";

    /**
     * A configurable table writer that will write all entries in a given table to a given output.
     */
    public static class TableWriter {

        /**
         * A function to format the {@link Key} of a row.
         */
        private final Function<Key,String> keyFormatter;

        /**
         * The function to format the timestamp of a row.
         */
        private final Function<Long,String> timestampFormatter;

        /**
         * The function to format the {@link Value} of a row.
         */
        private final BiFunction<Key,Value,String> valueFormatter;

        public TableWriter(Function<Key,String> keyFormatter, Function<Long,String> timestampFormatter, BiFunction<Key,Value,String> valueFormatter) {
            this.keyFormatter = keyFormatter;
            this.timestampFormatter = timestampFormatter;
            this.valueFormatter = valueFormatter;
        }

        /**
         * Writes each entry in the given table to the given output.
         *
         * @param client
         *            the client
         * @param authorizations
         *            the authorizations to use when scanning over the table
         * @param tableName
         *            the table name
         * @param output
         *            the output to write entries to
         * @throws TableNotFoundException
         *             if the table could not be found
         */
        public void writeTable(final AccumuloClient client, final Authorizations authorizations, final String tableName, final Output output)
                        throws TableNotFoundException {
            output.writeln("---- Begin table " + tableName + " ----");

            try (final Scanner scanner = client.createScanner(tableName, authorizations)) {
                for (final Map.Entry<Key,Value> entry : scanner) {
                    Key key = entry.getKey();
                    output.writeln(keyFormatter.apply(key) + " " + timestampFormatter.apply(key.getTimestamp()));
                    String valueAsString;
                    try {
                        valueAsString = valueFormatter.apply(key, entry.getValue());
                    } catch (Exception e) {
                        log.warn("Could not deserialize value for table " + tableName + "; key: " + key, e);
                        valueAsString = "(unable to deserialize value)";
                    }
                    output.write("\t" + valueAsString);
                }
            }

            output.writeln("---- End table " + tableName + " ----\n");
            output.flush();
        }
    }

    /**
     * A simple {@link TableWriter} with the following formatting configurations:
     * <ul>
     * <li>Key: formatted to the result of {@link Key#toStringNoTime()}.</li>
     * <li>Timestamp: formatted as a singular timestamp.</li>
     * <li>Value: formatted to the result of {@link Value#toString()}.</li>
     * </ul>
     */
    public static final TableWriter SIMPLE_TABLE_WRITER = new TableWriter(Key::toStringNoTime, PrintUtility::formatTimestamp,
                    (key, value) -> value == null || value.getSize() < 1 ? "" : value.toString());

    /**
     * A {@link TableWriter} for the shard table with the following formatting configurations:
     * <ul>
     * <li>Key: formatted to the result of {@link Key#toStringNoTime()}.</li>
     * <li>Timestamp: formatted as a composite timestamp.</li>
     * <li>Value: formatted depending on the column family:
     * <ul>
     * <li>Column Family {@code "tf"}: decoded as a {@link TermWeight.Info}.</li>
     * <li>Column Family {@code "d"}: decoded as a decompressed and decoded document.</li>
     * <li>All other rows: formatted to the result of {@link Value#toString()}.</li>
     * </ul>
     * </li>
     * </ul>
     */
    public static final TableWriter SHARD_TABLE_WRITER = new TableWriter(Key::toStringNoTime, PrintUtility::formatCompositeTimestamp, (key, value) -> {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        String colFam = key.getColumnFamily().toString();
        switch (colFam) {
            case "tf":
                // This is a term frequency row.
                try {
                    return decodeTermWeightInfo(value);
                } catch (Exception e) {
                    log.error("Failed to decode term weight for key: " + key, e);
                    return UNDECODED_VALUE;
                }
            case "d":
                // This is a compressed and encoded document row.
                try {
                    return decodeDocument(value);
                } catch (Exception e) {
                    log.error("Failed to decode document for key: " + key, e);
                    return UNDECODED_VALUE;
                }
            default:
                return value.toString();
        }
    });

    /**
     * A {@link TableWriter} for the shard index table with the following formatting configurations:
     * <ul>
     * <li>Key: formatted to the result of {@link Key#toStringNoTime()}.</li>
     * <li>Timestamp: formatted as a composite timestamp.</li>
     * <li>Value: decoded as a {@link Uid.List}.</li>
     * </ul>
     */
    public static final TableWriter SHARD_INDEX_TABLE_WRITER = new TableWriter(Key::toStringNoTime, PrintUtility::formatCompositeTimestamp, (key, value) -> {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            // This is a UID list.
            return decodeUidList(value);
        } catch (Exception e) {
            log.error("Failed to decode UID list for key: " + key, e);
            return UNDECODED_VALUE;
        }
    });

    /**
     * A {@link TableWriter} for the shard reverse index table. Identical to {@link #SHARD_INDEX_TABLE_WRITER}.
     *
     * @see #SHARD_INDEX_TABLE_WRITER
     */
    public static final TableWriter SHARD_RINDEX_TABLE_WRITER = SHARD_INDEX_TABLE_WRITER;

    /**
     * A {@link TableWriter} for the metadata table with the following formatting configurations:
     * <ul>
     * <li>Key: formatted to the result of {@link Key#toStringNoTime()}.</li>
     * <li>Timestamp: formatted as a singular timestamp.</li>
     * <li>Value: formatted depending on the column family:
     * <ul>
     * <li>Column Family {@code "f"}, {@code "r"}, or {@code "i"}: decoded as a long if non-aggregated, or a {@link DateFrequencyMap} if aggregrated.</li>
     * <li>Column Family {@code "edge"}: decoded as an {@link EdgeMetadata.MetadataValue}.</li>
     * <li>All other rows: formatted to the result of {@link Value#toString()}.</li>
     * </ul>
     * </li>
     * </ul>
     */
    public static final TableWriter METADATA_TABLE_WRITER = new TableWriter(Key::toStringNoTime, PrintUtility::formatTimestamp, (key, value) -> {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        String columnFamily = key.getColumnFamily().toString();
        switch (columnFamily) {
            case "f":
            case "i":
            case "ri":
                String colQualifier = key.getColumnQualifier().toString();
                int separatorPos = colQualifier.indexOf(NULL_BYTE);
                String remainder = (separatorPos == -1 ? "" : colQualifier.substring((separatorPos + 1)));
                if (remainder.equalsIgnoreCase(FrequencyMetadataAggregator.AGGREGATED)) {
                    try {
                        return decodeDateFrequencyMap(value);
                    } catch (Exception e) {
                        log.error("Failed to decode date frequency map for key: " + key, e);
                        return UNDECODED_VALUE;
                    }
                } else {
                    try {
                        return decodeLong(value);
                    } catch (Exception e) {
                        log.error("Failed to decode frequency value for key: " + key, e);
                        return UNDECODED_VALUE;
                    }
                }
            case "edge":
                try {
                    return decodeEdgeMetadata(value);
                } catch (Exception e) {
                    log.error("Failed to decode edge metadata value for key: " + key, e);
                    return UNDECODED_VALUE;
                }

            default:
                return value.toString();
        }
    });

    /**
     * A {@link TableWriter} for the model table. Identical to {@link #METADATA_TABLE_WRITER}.
     *
     * @see #METADATA_TABLE_WRITER
     */
    public static final TableWriter MODEL_TABLE_WRITER = METADATA_TABLE_WRITER;

    /**
     * A {@link TableWriter} for the date index table with the following formatting configurations:
     * <ul>
     * <li>Key: formatted to the result of {@link Key#toStringNoTime()}.</li>
     * <li>Timestamp: formatted as a singular timestamp.</li>
     * <li>Value: decoded as a {@link BitSet}.</li>
     * </ul>
     */
    public static final TableWriter DATE_INDEX_TABLE_WRITER = new TableWriter(Key::toStringNoTime, PrintUtility::formatTimestamp, (key, value) -> {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            return decodeBitset(value);
        } catch (Exception e) {
            log.error("Failed to decode bitset for key: " + key, e);
            return UNDECODED_VALUE;
        }
    });

    /**
     * A {@link TableWriter} for the facet table with the following formatting configurations:
     * <ul>
     * <li>Key: formatted to the result of {@link Key#toStringNoTime()}.</li>
     * <li>Timestamp: formatted as a singular timestamp.</li>
     * <li>Value: decoded as a {@link HyperLogLogPlus#cardinality()}.</li>
     * </ul>
     */
    public static final TableWriter FACET_TABLE_WRITER = new TableWriter(Key::toStringNoTime, PrintUtility::formatTimestamp, (key, value) -> {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            return decodeHyperLogLogPlusCardinality(value);
        } catch (Exception e) {
            log.error("Failed to decode hyperLogLogPlus cardinality for key: " + key, e);
            return UNDECODED_VALUE;
        }
    });

    /**
     * A {@link TableWriter} for the annotation table with the following formatting configurations:
     * <ul>
     * <li>Key: formatted to the result of {@link Key#toStringNoTime()}.</li>
     * <li>Timestamp: formatted as a singular timestamp.</li>
     * <li>Value: decoded as a {@link Segment}.</li>
     * </ul>
     */
    public static final TableWriter ANNOTATION_TABLE_WRITER = new TableWriter(Key::toStringNoTime, PrintUtility::formatTimestamp, (key, value) -> {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            return decodeAnnotationSegment(value);
        } catch (Exception e) {
            log.error("Failed to decode annotation segment for key: " + key, e);
            return UNDECODED_VALUE;
        }
    });

    /**
     * A {@link TableWriter} for the annotation source table with the following formatting configurations:
     * <ul>
     * <li>Key: formatted to the result of {@link Key#toStringNoTime()}.</li>
     * <li>Timestamp: formatted as a singular timestamp.</li>
     * <li>Value: decoded as a {@link AnnotationSource}.</li>
     * </ul>
     */
    public static final TableWriter ANNOTATION_SOURCE_TABLE_WRITER = new TableWriter(Key::toStringNoTime, PrintUtility::formatTimestamp, (key, value) -> {
        if (value == null || value.getSize() < 1) {
            return "";
        }

        try {
            return decodeAnnotationSource(value);
        } catch (Exception e) {
            log.error("Failed to decode annotation source for key: " + key, e);
            return UNDECODED_VALUE;
        }
    });

    /**
     * Writes the specified table using the given table writer to the log if the priority level DEBUG is enabled for the log.
     *
     * @param client
     *            the accumulo client
     * @param authorizations
     *            the authorizations to use when scanning over the table
     * @param tableName
     *            the table name
     * @param writer
     *            the table writer
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public static void printTableToLogDebug(final AccumuloClient client, final Authorizations authorizations, final String tableName, final TableWriter writer)
                    throws TableNotFoundException {
        if (log.isDebugEnabled()) {
            printTable(client, authorizations, tableName, writer, Output.ApacheLog4JOutput.debug(log));
        }
    }

    /**
     * Writes the specified table using the given table writer to the given output.
     *
     * @param client
     *            the accumulo client
     * @param authorizations
     *            the authorizations to use when scanning over the table
     * @param tableName
     *            the table name
     * @param writer
     *            the table writer
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public static void printTable(final AccumuloClient client, final Authorizations authorizations, final String tableName, final TableWriter writer,
                    Output output) throws TableNotFoundException {
        writer.writeTable(client, authorizations, tableName, output);
    }

    /**
     * Format the given timestamp.
     */
    private static String formatTimestamp(long timestamp) {
        return ISO_DATE_TIME_MILLIS.format(Instant.ofEpochMilli(timestamp));
    }

    /**
     * Return the formatted event and age-off date of the given composite timestamp.
     */
    private static String formatCompositeTimestamp(long timestamp) {
        return "Event date: " + formatTimestamp(CompositeTimestamp.getEventDate(timestamp)) + " Age-off date: "
                        + formatTimestamp(CompositeTimestamp.getAgeOffDate(timestamp));
    }

    /**
     * Decode the given value as an {@link DateFrequencyMap}.
     */
    private static String decodeDateFrequencyMap(Value value) {
        try {
            return new DateFrequencyMap(value.get()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as a long.
     */
    private static String decodeLong(Value value) {
        try {
            ByteArrayInputStream byteStream = new ByteArrayInputStream(value.get());
            DataInputStream dataInputStream = new DataInputStream(byteStream);
            return Long.toString(WritableUtils.readVLong(dataInputStream));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link HyperLogLogPlus} cardinality.
     */
    private static String decodeHyperLogLogPlusCardinality(Value value) {
        try {
            ExtendedHyperLogLogPlus hyperLogLogPlus = new ExtendedHyperLogLogPlus(value);
            return Long.toString(hyperLogLogPlus.getCardinality());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link Uid.List}.
     */
    private static String decodeUidList(Value value) {
        try {
            return Uid.List.parseFrom(value.get()).getUIDList().toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link TermWeight.Info}.
     */
    private static String decodeTermWeightInfo(Value value) {
        try {
            return TermWeight.Info.parseFrom(value.get()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link EdgeMetadata.MetadataValue}.
     */
    private static String decodeEdgeMetadata(Value value) {
        try {
            EdgeMetadata.MetadataValue metadataValue = EdgeMetadata.MetadataValue.parseFrom(value.get());
            return metadataValue.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an compressed and encoded document.
     */
    private static String decodeDocument(Value value) {
        try {
            final byte[] decodedContent = ContentKeyValueFactory.decodeAndDecompressContent(value.get());
            return new String(decodedContent, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link BitSet}.
     */
    private static String decodeBitset(Value value) {
        try {
            BitSet bitSet = BitSet.valueOf(value.get());
            return bitSet.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link Segment}.
     */
    private static String decodeAnnotationSegment(Value value) {
        try {
            return Segment.parseFrom(value.get()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link AnnotationSource}.
     */
    private static String decodeAnnotationSource(Value value) {
        try {
            return AnnotationSource.parseFrom(value.get()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PrintUtility() {
        throw new UnsupportedOperationException();
    }
}
