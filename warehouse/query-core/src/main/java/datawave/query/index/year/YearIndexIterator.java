package datawave.query.index.year;

import java.util.BitSet;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.jexl3.parser.ASTJexlScript;

import datawave.query.index.day.BitSetIndexEntry;
import datawave.query.index.day.DayIndexQueryBuilder;

/**
 * This iterator generates the shard offsets from Year index {@link BitSetIndexEntry}
 */
public class YearIndexIterator implements Iterator<String> {

    private final ASTJexlScript script;
    private Map<String,BitSet> shards;
    int index = 0;

    // avoid useless iterations
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;

    // the top key
    private String row;
    private String year;

    private final DayIndexQueryBuilder builder = new DayIndexQueryBuilder();

    public YearIndexIterator(ASTJexlScript script) {
        this.script = script;
    }

    public void setShards(Map<String,BitSet> shards) {
        this.shards = shards;

        if (this.shards.isEmpty()) {
            min = 0;
            max = 0;
        }

        // do some quick calculations to prevent extra iteration cycles
        for (BitSet bitSet : this.shards.values()) {
            int currentMin = bitSet.nextSetBit(0);
            int currentMax = bitSet.previousSetBit(bitSet.length());
            if (currentMin < min) {
                min = currentMin;
            }
            if (currentMax > max) {
                max = currentMax;
            }
        }

        index = min;
    }

    @Override
    public boolean hasNext() {
        if (row == null) {
            while (row == null && index <= max) {
                if (builder.buildQuery(script, shards, index) != null) {
                    // build the row
                    row = year + getMonthAndDay(index);
                }
                index++;
            }
        }
        return row != null;
    }

    private String getMonthAndDay(int index) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        calendar.set(Calendar.DAY_OF_YEAR, index);

        int month = 1 + calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);

        StringBuilder sb = new StringBuilder();
        if (month < 10) {
            sb.append('0').append(month);
        } else {
            sb.append(month);
        }

        if (day < 10) {
            sb.append('0').append(day);
        } else {
            sb.append(day);
        }

        return sb.toString();
    }

    @Override
    public String next() {
        String next = row;
        row = null;
        return next;
    }

    public void setYear(String year) {
        this.year = year;
    }
}
