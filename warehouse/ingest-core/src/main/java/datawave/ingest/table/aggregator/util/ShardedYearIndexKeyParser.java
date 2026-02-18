package datawave.ingest.table.aggregator.util;

import java.util.BitSet;
import java.util.Calendar;

import org.apache.accumulo.core.data.Key;

import datawave.util.time.DateHelper;

public class ShardedYearIndexKeyParser extends AbstractIndexKeyParser {

    @Override
    public Key convert() {
        if (isShardedYearKey()) {
            return key; // pass-through
        }

        // use a byte array constructor to avoid expensive parsing of the ColumnVisibility
        String year = getYear();
        byte[] row = (year + NULL_CHAR + getValue()).getBytes();
        byte[] cf = getField().getBytes();
        byte[] cq = getDatatype().getBytes();
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
