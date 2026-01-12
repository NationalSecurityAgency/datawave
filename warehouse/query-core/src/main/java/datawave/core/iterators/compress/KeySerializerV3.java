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
 * This serializer provides a further optimization for fields with grouping context, e.g. <code>FIELD.1.2</code>
 * <p>
 * <b>NOTE:</b> this class is final and should not be modified. You risk losing serialized events forever if you change this class. So please don't.
 */
public class KeySerializerV3 extends BaseKeySerializer {

    @Override
    public byte version() {
        return VERSION_THREE;
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

            String notation = null;
            int dotIndex = field.indexOf('.');
            if (dotIndex > 0) {
                // index + 1 to eliminate dot serialization.
                // just make sure to add it back
                notation = field.substring(dotIndex + 1);
                field = field.substring(0, dotIndex);
            }

            if (notation == null) {
                if (fieldOffsets.containsKey(field)) {
                    output.writeByte(FIELD_OFFSET);
                    output.writeInt(fieldOffsets.get(field), true);
                } else {
                    fieldOffsets.put(field, fieldOffsetIndex++);
                    output.writeByte(FIELD_VALUE);
                    output.writeString(field);
                }
            } else {
                if (fieldOffsets.containsKey(field)) {
                    output.writeByte(FIELD_NOTATION_OFFSET);
                    output.writeInt(fieldOffsets.get(field), true);
                    output.writeString(notation);
                } else {
                    fieldOffsets.put(field, fieldOffsetIndex++);
                    output.writeByte(FIELD_NOTATION_VALUE);
                    output.writeString(field);
                    output.writeString(notation);
                }
            }

            output.writeString(value);
        }
        output.flush();
    }

    public List<Key> readPayload(Input input, Key workKey) {

        byte[] row = workKey.getRowData().toArray();
        byte[] cf = workKey.getColumnFamilyData().toArray();
        byte[] cv = workKey.getColumnVisibilityData().toArray();
        long ts = workKey.getTimestamp();
        boolean deleted = workKey.isDeleted();

        int fieldOffsetIndex = 0;
        Map<Integer,String> fieldOffsets = new HashMap<>();

        int size = input.readInt(true);
        int offsetIndex;
        String field;
        String notation;
        String value;
        List<Key> keys = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {

            String cq;
            byte b = input.readByte();
            switch (b) {
                case FIELD_VALUE:
                    field = input.readString();
                    value = input.readString();
                    fieldOffsets.put(fieldOffsetIndex++, field);
                    cq = field + '\u0000' + value;
                    break;
                case FIELD_OFFSET:
                    offsetIndex = input.readInt(true);
                    field = fieldOffsets.get(offsetIndex);
                    value = input.readString();
                    cq = field + '\u0000' + value;
                    break;
                case FIELD_NOTATION_VALUE:
                    field = input.readString();
                    fieldOffsets.put(fieldOffsetIndex++, field);
                    notation = input.readString();
                    value = input.readString();
                    cq = field + '.' + notation + '\u0000' + value;
                    break;
                case FIELD_NOTATION_OFFSET:
                    offsetIndex = input.readInt(true);
                    field = fieldOffsets.get(offsetIndex);
                    notation = input.readString();
                    value = input.readString();
                    cq = field + '.' + notation + '\u0000' + value;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown byte: " + Integer.toHexString(b));
            }

            keys.add(new Key(row, cf, cq.getBytes(), cv, ts, deleted));
        }
        return keys;
    }
}
