package datawave.helpers;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.edge.util.ExtendedHyperLogLogPlus;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.Uid;
import datawave.metadata.protobuf.EdgeMetadata;
import datawave.query.model.DateFrequencyMap;
import datawave.query.table.parser.ContentKeyValueFactory;
import datawave.table.constants.TableName;
import datawave.util.CompositeTimestamp;

/**
 * A utility class for printing accumulo tables.
 */
public final class PrintUtility {

    private static final Logger log = LoggerFactory.getLogger(PrintUtility.class);

    /**
     * Print the table {@value TableName#SHARD} as a shard table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printShardTable(final AccumuloClient client, final Authorizations auths) throws TableNotFoundException {
        printShardTable(client, auths, TableName.SHARD);
    }

    /**
     * Print the specified table as a shard table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            sthe auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printShardTable(final AccumuloClient client, final Authorizations auths, final String tableName) throws TableNotFoundException {
        printTableToDebugLog(client, auths, tableName, new ShardTableWriter());
    }

    /**
     * Print the table {@value TableName#SHARD_INDEX} as a shard index table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printShardIndexTable(final AccumuloClient client, final Authorizations auths) throws TableNotFoundException {
        printShardIndexTable(client, auths, TableName.SHARD_INDEX);
    }

    /**
     * Print the specified table as a shard index table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printShardIndexTable(final AccumuloClient client, final Authorizations auths, final String tableName) throws TableNotFoundException {
        printTableToDebugLog(client, auths, tableName, new ShardIndexTableWriter());
    }

    /**
     * Print the table {@value TableName#SHARD_RINDEX} as a shard reverse index table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printShardRIndexTable(final AccumuloClient client, final Authorizations auths) throws TableNotFoundException {
        printShardRIndexTable(client, auths, TableName.SHARD_RINDEX);
    }

    /**
     * Print the specified table as a shard reverse index table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printShardRIndexTable(final AccumuloClient client, final Authorizations auths, final String tableName) throws TableNotFoundException {
        printShardIndexTable(client, auths, tableName);
    }

    /**
     * Print the table {@value TableName#DATE_INDEX} as a date index table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printDateIndexTable(final AccumuloClient client, final Authorizations auths) throws TableNotFoundException {
        printDateIndexTable(client, auths, TableName.DATE_INDEX);
    }

    /**
     * Print the specified table as a date index table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printDateIndexTable(final AccumuloClient client, final Authorizations auths, final String tableName) throws TableNotFoundException {
        printTableToDebugLog(client, auths, tableName, new DateIndexTableWriter());
    }

    /**
     * Print the table {@value TableName#METADATA} as a datawave metadata table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printMetadataTable(final AccumuloClient client, final Authorizations auths) throws TableNotFoundException {
        printMetadataTable(client, auths, TableName.METADATA);
    }

    /**
     * Print the specified table as a datawave metadata table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printMetadataTable(final AccumuloClient client, final Authorizations auths, final String tableName) throws TableNotFoundException {
        printTableToDebugLog(client, auths, tableName, new MetadataTableWriter());
    }

    /**
     * Print the specified table as a facet table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printFacetTable(final AccumuloClient client, final Authorizations auths, final String tableName) throws TableNotFoundException {
        printTableToDebugLog(client, auths, tableName, new FacetTableWriter());
    }

    /**
     * Print the specified table as an annotation table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printAnnotationTable(final AccumuloClient client, final Authorizations auths, final String tableName) throws TableNotFoundException {
        printTableToDebugLog(client, auths, tableName, new AnnotationTableWriter());
    }

    /**
     * Print the specified table as an annotation source table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @throws TableNotFoundException
     *             if the table could not be found
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printAnnotationSourceTable(final AccumuloClient client, final Authorizations auths, final String tableName)
                    throws TableNotFoundException {
        printTableToDebugLog(client, auths, tableName, new AnnotationSourceTableWriter());
    }

    /**
     * Print the specified table as a simple table.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @throws TableNotFoundException
     *             if the table does not exist
     * @see #printTableToDebugLog(AccumuloClient, Authorizations, String, TableWriter)
     */
    public static void printSimpleTable(final AccumuloClient client, final Authorizations auths, final String tableName) throws TableNotFoundException {
        printTableToDebugLog(client, auths, tableName, new TableWriter());
    }

    /**
     * Print the specified table using the given table writer to the {@link PrintUtility}'s logger as DEBUG statements if DEBUG is enabled.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the auths to use when scanning over the table
     * @param tableName
     *            the table name
     * @param writer
     *            the table writer
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public static void printTableToDebugLog(final AccumuloClient client, final Authorizations auths, final String tableName, final TableWriter writer)
                    throws TableNotFoundException {
        if (log.isDebugEnabled()) {
            writer.writeTable(client, auths, tableName, Slf4jOutput.debug(log));
        }
    }

    private static final DateTimeFormatter ISO_DATE_TIME_MILLIS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneId.systemDefault());

    /**
     * Format the given timestamp.
     */
    public static String formatTimestamp(long timestamp) {
        return ISO_DATE_TIME_MILLIS.format(Instant.ofEpochMilli(timestamp));
    }

    /**
     * Return the formatted event and age-off date of the given composite timestamp.
     */
    public static String formatCompositeTimestamp(long timestamp) {
        return "Event date: " + formatTimestamp(CompositeTimestamp.getEventDate(timestamp)) + " Age-off date: "
                        + formatTimestamp(CompositeTimestamp.getAgeOffDate(timestamp));
    }

    /**
     * Decode the given value as an {@link DateFrequencyMap}.
     */
    public static String decodeDateFrequencyMap(Value value) {
        try {
            return new DateFrequencyMap(value.get()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as a long.
     */
    public static String decodeLong(Value value) {
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
    public static String decodeHyperLogLogPlusCardinality(Value value) {
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
    public static String decodeUidList(Value value) {
        try {
            return Uid.List.parseFrom(value.get()).getUIDList().toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link TermWeight.Info}.
     */
    public static String decodeTermWeightInfo(Value value) {
        try {
            return TermWeight.Info.parseFrom(value.get()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link EdgeMetadata.MetadataValue}.
     */
    public static String decodeEdgeMetadata(Value value) {
        try {
            EdgeMetadata.MetadataValue metadataValue = EdgeMetadata.MetadataValue.parseFrom(value.get());
            return metadataValue.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as a compressed and encoded document.
     */
    public static String decodeDocument(Value value) {
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
    public static String decodeBitset(Value value) {
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
    public static String decodeAnnotationSegment(Value value) {
        try {
            return Segment.parseFrom(value.get()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode the given value as an {@link AnnotationSource}.
     */
    public static String decodeAnnotationSource(Value value) {
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
