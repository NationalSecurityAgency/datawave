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

        String year = getYear();
        String nextRow = year + NULL_CHAR + getValue();
        return new Key(nextRow, getField(), getDatatype(), key.getColumnVisibilityParsed(), key.getTimestamp());
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
