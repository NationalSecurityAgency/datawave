package datawave.ingest.table.filter;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import datawave.data.ColumnFamilyConstants;

/**
 * {@link Filter} implementation that will filter out any whindex entries ("{@code wcd}" column family) that do not contain the earliest date present in the
 * column qualifier (the creation date). See below for an example:
 *
 * <pre>
 * INPUT ROWS:
 *
 * row_id   cf   cq
 * ----------------
 * APPLE    f    csv\020200301
 * APPLE    i    csv\020200301
 * APPLE    wcd  csv\020200301
 * APPLE    wcd  csv\020200311
 * APPLE    wcd  csv\020200420
 *
 *
 * OUTPUT ROWS:
 *
 * row_id   cf   cq
 * ----------------
 * APPLE    f    csv\020200301
 * APPLE    i    csv\020200301
 * APPLE    wcd  csv\020200301
 * </pre>
 */
public class WhindexCreationDateFilter extends Filter {

    private static final String NULL_BYTE = "\0";

    Text prevRowID = null;
    Text prevCF = null;
    String prevDataType = null;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
    }

    /**
     * Returns whether the entry should be accepted by the filter. Whindex entries ("{@code wcd}") that do not contain the earliest creation date will be
     * filtered out.
     *
     * @param k
     *            the current key
     * @param v
     *            the value associated with the {@code key}
     * @return {@code true} if the entry is either the earliest whindex entry (by creation date) for the row/datatype combo seen in the key, or if the entry is
     *         a non-whindex entry. Otherwise, returns false.
     */
    @Override
    public boolean accept(Key k, Value v) {
        Text currentRow = k.getRow();
        // We've encountered a new row ID. Reset all tracking variables.
        if (!Objects.equals(prevRowID, currentRow)) {
            prevRowID = currentRow;
            prevCF = null;
            prevDataType = null;
        }

        Text currentCF = k.getColumnFamily();
        // We've encountered a new column family. Reset the datatype tracking variable.
        if (Objects.equals(prevCF, currentCF)) {
            prevCF = currentCF;
            prevDataType = null;
        }

        // We've encountered a non-whindex column family. Never filter this out.
        if (!currentCF.equals(ColumnFamilyConstants.COLF_WCD)) {
            return true;
        }

        // Extract the data type.
        String colq = k.getColumnQualifier().toString();
        int nullBytePos = colq.indexOf(NULL_BYTE);
        String currDataType = colq.substring(0, nullBytePos);

        // We've encountered a new datatype. The first wcd entry seen for a datatype always contains the earliest
        // creation date.
        if (!Objects.equals(prevDataType, currDataType)) {
            prevDataType = currDataType;
            return true;
        } else {
            // This is a subsequent wcd entry for the current row/datatype. Do not include.
            return false;
        }
    }
}
