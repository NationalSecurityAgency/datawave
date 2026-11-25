package datawave.helpers;

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple accumulo table writer.
 */
public class TableWriter {

    protected static final String UNDECODED_VALUE = "(unable to decode value)";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Print the specified table to the output.
     *
     * @param client
     *            the accumulo client
     * @param auths
     *            the authorizations
     * @param tableName
     *            the table name
     * @param output
     *            the output to write the table to
     * @throws TableNotFoundException
     *             if the table cannot be found
     */
    public void writeTable(final AccumuloClient client, final Authorizations auths, final String tableName, final Output output) throws TableNotFoundException {
        output.writeln("---- Begin table " + tableName + " ----");

        try (final Scanner scanner = client.createScanner(tableName, auths)) {
            // Write each entry in the table.
            for (final Map.Entry<Key,Value> entry : scanner) {
                // Write the key and timestamp.
                Key key = entry.getKey();
                output.writeln(formatKey(key) + " " + formatTimestamp(key.getTimestamp()));
                // Write the value.
                String valueAsString;
                try {
                    valueAsString = formatValue(key, entry.getValue());
                } catch (Exception e) {
                    log.warn("Could not deserialize value for table {}; key; {}", tableName, key, e);
                    valueAsString = "(unable to deserialize value)";
                }
                output.write("\t" + valueAsString);
            }
        }

        output.writeln("---- End table " + tableName + " ----\n");
        output.flush();
    }

    /**
     * Return a formatted key.
     *
     * @param key
     *            the key to format
     * @return the formatted key
     */
    protected String formatKey(Key key) {
        return key.toStringNoTime();
    }

    /**
     * Return a formatted timestamp.
     *
     * @param timestamp
     *            the timestamp to format
     * @return the formatted timestamp
     */
    protected String formatTimestamp(long timestamp) {
        return PrintUtility.formatTimestamp(timestamp);
    }

    /**
     * Return a formatted value, or an empty string if the value is null or has a size less than 1.
     *
     * @param key
     *            the key
     * @param value
     *            the value
     * @return the formatted value
     */
    protected String formatValue(Key key, Value value) {
        if (value == null || value.getSize() < 1) {
            return "";
        }
        return value.toString();
    }
}
