package datawave.query.index.day;

import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.planner.QueryPlan;

/**
 * This iterator generates the per-shard query plans given the bitset entries in the day index.
 */
public class DayIndexIterator implements Iterator<QueryPlan> {

    private final ASTJexlScript script;
    private Map<String,BitSet> shards;
    int index = 0;

    // avoid useless iterations
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;

    // the top key
    private JexlNode node;
    private String day;

    private final DayIndexQueryBuilder builder = new DayIndexQueryBuilder();

    public DayIndexIterator(ASTJexlScript script) {
        this.script = script;
    }

    public void setShards(Map<String,BitSet> shards) {
        this.shards = shards;

        if (shards.isEmpty()) {
            min = 0;
            max = 0;
        }

        // do some quick calculations to prevent extra iteration cycles
        for (BitSet bitSet : shards.values()) {
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
        if (node == null) {
            while (node == null && index <= max) {
                // TODO -- maybe do next set bit on values again? could matter for sparse data.
                node = builder.buildQuery(script, shards, index);
                index++;
            }
        }
        return node != null;
    }

    @Override
    public QueryPlan next() {
        JexlNode next = node;
        node = null;

        //  @formatter:off
        return new QueryPlan()
                        .withQueryTree(next)
                        .withRanges(Collections.singleton(Range.exact(day + "_" + (index-1))));
        //  @formatter:on
    }

    public void setDay(String day) {
        this.day = day;
    }
}
