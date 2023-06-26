package datawave.ingest.mapreduce.job.metrics;

import datawave.util.TextUtil;
import org.apache.accumulo.core.data.Key;

/**
 * For performance, we are using Strings to represent Accumulo Keys until we finally flush to the ContextWriter. This class handles the conversions.
 */
public class KeyConverter {

    private static final String DELIM = "\u0001";
    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Converts the pieces of a key to a delimited string.
     *
     * @param row
     *            the event row
     * @param qual
     *            the column qualifer
     * @param fam
     *            the column family
     * @param viz
     *            the column visibility
     * @return string representation
     */
    public static String toString(String row, String qual, String fam, String viz) {
        return row + DELIM + qual + DELIM + fam + DELIM + viz;
    }

    /**
     * Converts from the delimited string into an Accumulo key
     *
     * @param str
     *            the string input
     * @return Accumulo key
     */
    public static Key fromString(String str) {
        String[] tokens = str.split(DELIM);

        byte[] row = TextUtil.toUtf8(tokens[0]);
        byte[] fam = TextUtil.toUtf8(tokens[1]);
        byte[] qual = TextUtil.toUtf8(tokens[2]);
        byte[] viz = (tokens.length > 3 ? TextUtil.toUtf8(tokens[3]) : EMPTY_BYTES);

        return new Key(row, fam, qual, viz, Long.MAX_VALUE);

    }
}
