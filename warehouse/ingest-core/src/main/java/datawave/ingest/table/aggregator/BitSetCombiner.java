package datawave.ingest.table.aggregator;

import java.util.BitSet;
import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import datawave.util.TableName;

/**
 * A combiner that operates on the {@link TableName#SHARD_YEAR_INDEX} and {@link TableName#SHARD_DAY_INDEX}.
 * <p>
 * The value is a single bitset. In the year index the bits map to days, in the day index the bits map to shards
 * <p>
 * Deletes are not honored.
 */
public class BitSetCombiner extends Combiner {

    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        BitSet bits = new BitSet();

        while (iter.hasNext()) {
            Value value = iter.next();
            byte[] bytes = value.get();
            BitSet candidate = BitSet.valueOf(bytes);
            bits.or(candidate);
        }

        return new Value(bits.toByteArray());
    }

}
