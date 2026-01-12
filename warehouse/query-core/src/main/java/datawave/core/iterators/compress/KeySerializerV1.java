package datawave.core.iterators.compress;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A {@link KeySerializer} that serializes the full field and value with no additional optimizations.
 * <p>
 * <b>NOTE:</b> this class is final and should not be modified. You risk losing serialized events forever if you change this class. So please don't.
 */
public final class KeySerializerV1 extends BaseKeySerializer {

    @Override
    public byte version() {
        return VERSION_ONE;
    }

    @Override
    public void writePayload(Output output, List<Key> keys) {
        output.writeInt(keys.size(), true);
        for (Key key : keys) {
            output.writeString(key.getColumnQualifier().toString());
        }
    }

    @Override
    public List<Key> readPayload(Input input, Key workKey) {
        byte[] row = workKey.getRowData().toArray();
        byte[] cf = workKey.getColumnFamilyData().toArray();
        byte[] cv = workKey.getColumnVisibilityData().toArray();
        long ts = workKey.getTimestamp();
        boolean deleted = workKey.isDeleted();

        int size = input.readInt(true);
        List<Key> keys = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String cq = input.readString();
            if (cq != null && cv != null) {
                keys.add(new Key(row, cf, cq.getBytes(), cv, ts, deleted));
            }
        }
        return keys;
    }
}
