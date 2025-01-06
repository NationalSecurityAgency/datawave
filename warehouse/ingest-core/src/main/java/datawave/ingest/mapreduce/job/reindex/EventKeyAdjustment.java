package datawave.ingest.mapreduce.job.reindex;

import java.util.function.Function;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;

public class EventKeyAdjustment implements Function<Key,Key> {
    @Override
    public Key apply(Key key) {
        ByteSequence cf = key.getColumnFamilyData();
        if (!ShardReindexMapper.isKeyD(cf) && !ShardReindexMapper.isKeyTF(cf) && !ShardReindexMapper.isKeyFI(cf) && ShardReindexMapper.isKeyEvent(cf)) {
            return shift(key);
        }

        // nothing to do
        return key;
    }

    // move to the next event
    protected Key shift(Key key) {
        return key.followingKey(PartialKey.ROW_COLFAM);
    }
}
