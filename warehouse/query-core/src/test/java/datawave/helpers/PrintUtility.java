package datawave.helpers;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.edge.util.EdgeValue;
import datawave.edge.util.ExtendedHyperLogLogPlus;
import datawave.ingest.protobuf.Uid;
import datawave.util.CompositeTimestamp;

/**
 * A set of static methods for printing tables in mock Accumulo instance.
 */
public class PrintUtility {

    private static final Logger logger = Logger.getLogger(PrintUtility.class);

    /**
     * Hide default constructor
     */
    private PrintUtility() {
        // Nothing to do
    }

    /**
     * Utility class to print all the entries in a table
     *
     * @param client
     *            Connector to mock accumulo
     * @param authorizations
     *            Authorizations to run scanner with
     * @param tableName
     *            Table to scan
     * @throws TableNotFoundException
     *             Invalid table name
     */
    public static void printTable(final AccumuloClient client, final Authorizations authorizations, final String tableName) throws TableNotFoundException {
        if (logger.isDebugEnabled()) {
            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");

            final StringBuilder sb = new StringBuilder("\n--Begin entire " + tableName + " table--");

            sb.append("\n");

            final Scanner scanner = client.createScanner(tableName, authorizations);
            for (final Entry<Key,Value> e : scanner) {
                sb.append(e.getKey().toStringNoTime());
                sb.append(" EventDate ");
                sb.append(dateFormat.format(new Date(CompositeTimestamp.getEventDate(e.getKey().getTimestamp()))));
                sb.append(" AgeOffDate ");
                sb.append(dateFormat.format(new Date(CompositeTimestamp.getAgeOffDate(e.getKey().getTimestamp()))));
                sb.append('\t');
                sb.append(getPrintableValue(tableName, e.getKey(), e.getValue()));
                sb.append("\n");
            }

            sb.append("--End entire ").append(tableName).append(" table--").append("\n");

            logger.debug(sb.toString());
        }
    }

    public static void printTable(final AccumuloClient client, final Authorizations authorizations, final String tableName, final PrintStream out)
                    throws TableNotFoundException {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");

        final StringBuilder sb = new StringBuilder("--Begin entire " + tableName + " table--");

        sb.append("\n");

        final Scanner scanner = client.createScanner(tableName, authorizations);
        for (final Entry<Key,Value> e : scanner) {
            sb.append(e.getKey().toStringNoTime());
            sb.append(" EventDate ");
            sb.append(dateFormat.format(new Date(CompositeTimestamp.getEventDate(e.getKey().getTimestamp()))));
            sb.append(" AgeOffDate ");
            sb.append(dateFormat.format(new Date(CompositeTimestamp.getAgeOffDate(e.getKey().getTimestamp()))));
            sb.append('\t');
            sb.append(getPrintableValue(tableName, e.getKey(), e.getValue()));
            sb.append("\n");
        }

        sb.append("--End entire ").append(tableName).append(" table--").append("\n");

        out.println(sb);
    }

    /**
     * List all the mock tables
     *
     * @param client
     *            Connector to mock accumulo
     */
    public static void listTables(final AccumuloClient client) {
        final StringBuilder sb = new StringBuilder("--Begin tables list--\n");

        for (final String tableName : client.tableOperations().list()) {
            sb.append(tableName).append('\n');
        }

        sb.append("--End table list--").append("\n");

        logger.debug(sb.toString());
    }

    public static String getPrintableValue(final String tableName, final Key key, final Value value) {
        if ((value == null) || value.getSize() < 1) {
            return "";
        }

        String lastError = "";

        try {
            final Uid.List uidList = Uid.List.parseFrom(value.get());
            return (uidList.getUIDList().toString());
        } catch (final InvalidProtocolBufferException e) {
            logger.trace("Deserialization as Uid.List, trying other methods", e);
            lastError = e.getMessage();
        }

        try {
            return (ReflectionToStringBuilder.toString(EdgeValue.decode(value), ToStringStyle.SHORT_PREFIX_STYLE));
        } catch (final Exception e) {
            logger.trace("Deserialization as decodedEdgeValue failed, trying other methods", e);
            lastError = e.getMessage();
        }

        try {
            final ExtendedHyperLogLogPlus ehllp = new ExtendedHyperLogLogPlus(value);
            return (String.valueOf(ehllp.getCardinality()));
        } catch (final Exception e) {
            logger.trace("Deserialization as ExtendedHyperLogLogPlus failed, trying other methods", e);
            lastError = e.getMessage();
        }

        try {
            ByteArrayInputStream byteStream = new ByteArrayInputStream(value.get());
            DataInputStream inputStream = new DataInputStream(byteStream);
            return String.valueOf(WritableUtils.readVLong(inputStream));
        } catch (final Exception e) {
            logger.trace("Deserialization as long value failed", e);
            lastError = e.getMessage();
        }

        // TODO: Metadata table?

        logger.warn("Could not deserialize protobuff for table '" + tableName + "'; key: '" + key.toString() + "' error: '" + lastError + "'");

        return "(unable to deserialize value)";
    }
}
