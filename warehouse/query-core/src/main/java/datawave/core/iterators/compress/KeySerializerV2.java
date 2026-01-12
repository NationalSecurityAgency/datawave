package datawave.core.iterators.compress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A {@link KeySerializer} tha serializes the full field once and offset for every additional instance. The full value is always serialized.
 * <p>
 * <b>NOTE:</b> this class is final and should not be modified. You risk losing serialized events forever if you change this class. So please don't.
 */
public final class KeySerializerV2 extends BaseKeySerializer {

    @Override
    public byte version() {
        return VERSION_TWO;
    }

    @Override
    public void writePayload(Output output, List<Key> keys) {
        output.writeInt(keys.size(), true);

        int fieldOffsetIndex = 0;
        Map<String,Integer> fieldOffsets = new HashMap<>();
        for (Key key : keys) {
            String cq = key.getColumnQualifier().toString();
            int nullIndex = cq.indexOf('\u0000');
            String field = cq.substring(0, nullIndex);
            String value = cq.substring(nullIndex + 1);

            if (fieldOffsets.containsKey(field)) {
                output.writeByte(FIELD_OFFSET);
                output.writeInt(fieldOffsets.get(field), true);
            } else {
                fieldOffsets.put(field, fieldOffsetIndex++);
                output.writeByte(FIELD_VALUE);
                output.writeString(field);
            }
            output.writeString(value);
        }
        output.flush();
    }

    @Override
    public List<Key> readPayload(Input input, Key workKey) {

        byte[] row = workKey.getRowData().toArray();
        byte[] cf = workKey.getColumnFamilyData().toArray();
        byte[] cv = workKey.getColumnVisibilityData().toArray();
        long ts = workKey.getTimestamp();
        boolean deleted = workKey.isDeleted();

        int fieldOffsetIndex = 0;
        Map<Integer,String> fieldOffsets = new HashMap<>();

        int size = input.readInt(true);
        List<Key> keys = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {

            String cq;
            byte b = input.readByte();
            switch (b) {
                case FIELD_VALUE:
                    String field = input.readString();
                    fieldOffsets.put(fieldOffsetIndex++, field);
                    cq = field + '\u0000' + input.readString();
                    break;
                case FIELD_OFFSET:
                    int index = input.readInt(true);
                    cq = fieldOffsets.get(index) + '\u0000' + input.readString();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown byte: " + Integer.toHexString(b));
            }

            keys.add(new Key(row, cf, cq.getBytes(), cv, ts, deleted));
        }
        return keys;
    }
}
