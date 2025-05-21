package datawave.query.index.lookup;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.index.lookup.RangeStream.NumShardFinder;
import datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType;
import datawave.query.util.Tuple2;
import datawave.util.time.DateHelper;

/**
 * This iterator presents a shard-specific view of an index scan, given the date range and the number of shards per day
 * <p>
 * Terms such as {@link MarkerType#EXCEEDED_VALUE} or {@link MarkerType#EXCEEDED_OR} are not executable against the global index for the purposes of shard
 * nomination. In these cases this iterator is used to present a full index scan list of shards for maximum fan out.
 * <p>
 * Functionally similar to the {@link HintToShardIterator}
 */
public class ShardSpecificIndexIterator implements Iterator<Tuple2<String,IndexInfo>> {

    private final JexlNode node;
    private final NumShardFinder numShardFinder;
    private final Calendar start;
    private final Calendar stop;

    private String top = null;
    private String day = null;
    private final TreeSet<String> shards = new TreeSet<>();

    public ShardSpecificIndexIterator(JexlNode node, NumShardFinder numShardFinder, Date start, Date stop) {
        this.node = node;
        this.numShardFinder = numShardFinder;
        this.start = getCalendarStartOfDay(start);
        this.stop = getCalendarStartOfDay(stop);

        day = DateHelper.format(start);
        getShardsForDay();
    }

    @Override
    public boolean hasNext() {
        if (top == null) {
            if (day == null) {
                getNextDay();
                if (day != null) {
                    getShardsForDay();
                }
            }

            if (!shards.isEmpty()) {
                top = shards.pollFirst();
                if (shards.isEmpty()) {
                    day = null;
                }
            }
        }
        return top != null;
    }

    private void getNextDay() {
        if (day == null) {
            if (start.compareTo(stop) < 0) {
                start.add(Calendar.DAY_OF_YEAR, 1);
                day = DateHelper.format(start.getTime());
            }
        }
    }

    private void getShardsForDay() {
        shards.clear();
        int num = numShardFinder.getNumShards(day);
        for (int i = 0; i < num; i++) {
            shards.add(day + "_" + i);
        }
    }

    @Override
    public Tuple2<String,IndexInfo> next() {
        String tk = top;
        top = null;
        return new Tuple2<>(tk, getIndexInfo());
    }

    private IndexInfo getIndexInfo() {
        IndexInfo info = new IndexInfo(-1);
        info.setNode(node);
        return info;
    }

    private static Calendar getCalendarStartOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }
}
