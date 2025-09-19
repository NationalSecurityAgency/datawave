package datawave.query.index.lookup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.Constants;
import datawave.query.index.lookup.RangeStream.NumShardFinder;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.Tuple2;

/**
 * Presents a 'shard-only' view of the underlying list of shard and day hints
 * <p>
 * This iterator accepts an arbitrary number of hints (day or shard) along with a mechanism to determine the number of shards in a given day.
 * <p>
 * When a day range is present and the number of shards is known this iterator will generate all possible shards for that day. This preserves the ability to
 * conduct searches across a day in parallel.
 */
public class HintToShardIterator implements Iterator<Tuple2<String,IndexInfo>> {

    private final List<String> elements;
    private final NumShardFinder numShardFinder;

    // state
    private String top;
    private boolean isTopDay = false;
    private final TreeSet<String> shards = new TreeSet<>();

    public HintToShardIterator(String[] hints, NumShardFinder numShardFinder) {
        elements = new ArrayList<>();
        Collections.addAll(elements, hints);
        this.numShardFinder = numShardFinder;
    }

    @Override
    public boolean hasNext() {
        if (top == null) {
            if (!elements.isEmpty()) {
                top = elements.remove(0);
                isTopDay = isDayHint(top);

                if (isTopDay) {
                    // populate shards
                    int num = numShardFinder.getNumShards(top);
                    for (int i = 0; i < num; i++) {
                        shards.add(top + "_" + i);
                    }
                }
            }
        }

        return top != null;
    }

    private boolean isDayHint(String hint) {
        return !hint.contains("_");
    }

    @Override
    public Tuple2<String,IndexInfo> next() {
        if (top == null) {
            throw new RuntimeException("Tried calling next when no top element existed");
        }

        String hint;
        if (isTopDay) {
            // get next shard
            hint = getNextShard();

            if (shards.isEmpty()) {
                top = null;
            }

            return new Tuple2<>(hint, getIndexInfo(hint));
        }

        hint = top;
        Tuple2<String,IndexInfo> next = new Tuple2<>(hint, getIndexInfo(hint));
        top = null;
        return next;
    }

    private IndexInfo getIndexInfo(String hint) {
        JexlNode assignment = JexlNodeFactory.createAssignment(Constants.SHARD_DAY_HINT, hint);
        JexlNode node = JexlNodeFactory.createExpression(assignment);

        IndexInfo info = new IndexInfo(-1);
        info.setNode(node);
        return info;
    }

    private String getNextShard() {
        if (shards.isEmpty()) {
            // then we have a day shard and no num shards cache. just return the top element
            return top;
        }

        // pop and go
        return shards.pollFirst();
    }
}
