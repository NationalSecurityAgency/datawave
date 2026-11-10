package datawave.ingest.table.aggregator.util;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Calendar;

import org.apache.accumulo.core.data.Key;

import datawave.util.time.DateHelper;

@Deprecated(forRemoval = true, since = "7.40.0")
public class ShardedYearIndexKeyParser extends AbstractIndexKeyParser {

    @Override
    public Key convert() {
        if (isShardedYearKey()) {
            return key; // pass-through
        }

        // use a byte array constructor to avoid expensive parsing of the ColumnVisibility
        String year = getYear();
        byte[] row = (year + NULL_CHAR + getValue()).getBytes(StandardCharsets.UTF_8);
        byte[] cf = getField().getBytes(StandardCharsets.UTF_8);
        byte[] cq = getDatatype().getBytes(StandardCharsets.UTF_8);
        byte[] cv = key.getColumnVisibilityData().toArray();
        return new Key(row, cf, cq, cv, key.getTimestamp());
    }

    public BitSet getBitset() {
        if (isShardedYearKey()) {
            return null;
        }

        if (bitset == null) {
            String date = getDate();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(DateHelper.parse(date));

            int offset = calendar.get(Calendar.DAY_OF_YEAR);
            bitset = new BitSet();
            bitset.set(offset);
        }
        return bitset;
    }

    protected String getYear() {
        String date = getDate();
        return date.substring(0, 4);
    }
}
