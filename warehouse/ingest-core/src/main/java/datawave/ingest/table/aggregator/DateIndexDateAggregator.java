package datawave.ingest.table.aggregator;

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Implementation of an Aggregator that aggregates objects of the type DateIndex.List. A date index entry contains a list of dates and shard specs. A date is of
 * the form yyyyMMdd, and a shard spec is a bit set representing the shards
 *
 *
 */
public class DateIndexDateAggregator extends PropogatingCombiner {

    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        byte[] finalBytes = new byte[0];
        int len = finalBytes.length;
        while (iter.hasNext()) {
            byte[] nextBytes = iter.next().get();
            len = (nextBytes.length > len ? nextBytes.length : len);
            if (len > finalBytes.length) {
                byte[] newBytes = new byte[len];
                if (finalBytes.length > 0) {
                    System.arraycopy(finalBytes, 0, newBytes, 0, finalBytes.length);
                }
                finalBytes = newBytes;
            }
            for (int i = 0; i < nextBytes.length; i++) {
                finalBytes[i] = (byte) (((finalBytes[i] & 0xff) | (nextBytes[i]) & 0xff));
            }
        }
        if (len == 0) {
            return new Value();
        } else {
            return new Value(finalBytes, false);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.table.aggregator.PropogatingAggregator#propogateKey()
     */
    @Override
    public boolean propogateKey() {
        return true;
    }
}
