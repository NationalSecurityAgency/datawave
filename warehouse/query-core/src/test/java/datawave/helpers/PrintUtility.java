package datawave.helpers;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.edge.util.EdgeValue;
import datawave.edge.util.ExtendedHyperLogLogPlus;
import datawave.ingest.protobuf.Uid;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Logger;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

/**
 * A set of static methods for printing tables in mock Accumulo instance.
 */
public class PrintUtility {
    
    private static Logger logger = Logger.getLogger(PrintUtility.class);
    
    /**
     * Hide default constructor
     */
    private PrintUtility() {
        // Nothing to do
    }
    
    /**
     * Utility class to print all the entries in a table
     *
     * @param conn
     *            Connector to mock accumulo
     * @param authorizations
     *            Authorizations to run scanner with
     * @param tableName
     *            Table to scan
     * @throws TableNotFoundException
     *             Invalid table name
     */
    public static void printTable(final Connector conn, final Authorizations authorizations, final String tableName) throws TableNotFoundException {
        if (logger.isDebugEnabled()) {
            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
            
            final StringBuilder sb = new StringBuilder("--Begin entire " + tableName + " table--");
            
            sb.append("\n");
            
            final Scanner scanner = conn.createScanner(tableName, authorizations);
            for (final Entry<Key,Value> e : scanner) {
                sb.append(e.getKey().toStringNoTime());
                sb.append(' ');
                sb.append(dateFormat.format(new Date(e.getKey().getTimestamp())));
                sb.append('\t');
                sb.append(getPrintableValue(e.getValue()));
                sb.append("\n");
            }
            
            sb.append("--End entire ").append(tableName).append(" table--").append("\n");
            
            logger.debug(sb.toString());
        }
    }
    
    public static void printTable(final Connector conn, final Authorizations authorizations, final String tableName, final PrintStream out)
                    throws TableNotFoundException {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
        
        final StringBuilder sb = new StringBuilder("--Begin entire " + tableName + " table--");
        
        sb.append("\n");
        
        final Scanner scanner = conn.createScanner(tableName, authorizations);
        for (final Entry<Key,Value> e : scanner) {
            sb.append(e.getKey().toStringNoTime());
            sb.append(' ');
            sb.append(dateFormat.format(new Date(e.getKey().getTimestamp())));
            sb.append('\t');
            sb.append(getPrintableValue(e.getValue()));
            sb.append("\n");
        }
        
        sb.append("--End entire ").append(tableName).append(" table--").append("\n");
        
        out.println(sb);
    }
    
    /**
     * List all the mock tables
     *
     * @param conn
     *            Connector to mock accumulo
     */
    public static void listTables(final Connector conn) {
        final StringBuilder sb = new StringBuilder("--Begin tables list--\n");
        
        for (final String tableName : conn.tableOperations().list()) {
            sb.append(tableName).append('\n');
        }
        
        sb.append("--End table list--").append("\n");
        
        logger.debug(sb.toString());
    }
    
    public static String getPrintableValue(final Value value) {
        if ((value != null) && (value.getSize() > 0)) {
            try {
                final Uid.List uidList = Uid.List.parseFrom(value.get());
                
                return (uidList.getUIDList().toString());
            } catch (final InvalidProtocolBufferException e1) {
                try {
                    return (ReflectionToStringBuilder.toString(EdgeValue.decode(value), ToStringStyle.SHORT_PREFIX_STYLE));
                } catch (final Exception e2) {
                    try {
                        final ExtendedHyperLogLogPlus ehllp = new ExtendedHyperLogLogPlus(value);
                        
                        return (String.valueOf(ehllp.getCardinality()));
                    } catch (final Exception e3) {
                        logger.error("Could not deserialize protobuff" + e2);
                    }
                }
            }
        }
        
        return ("");
    }
}
