package datawave.core.iterators.compress;

import static datawave.core.iterators.compress.BaseKeySerializer.VERSION_ONE;
import static datawave.core.iterators.compress.BaseKeySerializer.VERSION_THREE;
import static datawave.core.iterators.compress.BaseKeySerializer.VERSION_TWO;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.accumulo.core.data.Key;

import com.esotericsoftware.kryo.io.Input;
import com.google.common.base.Preconditions;

/**
 * A serializer that can dynamically read any supported serialization schema via the {@link VersionedKeySerializer#version()}.
 */
@NotThreadSafe
public class MetaKeySerializer implements KeySerializer {

    //  @formatter:off
    //  NOTE: this collection can only be added to. Removing an element or modified the order risks data loss.
    private final VersionedKeySerializer[] serializers = List.of(
            new KeySerializerV1(),
            new KeySerializerV2(),
            new KeySerializerV3()
    ).toArray(new VersionedKeySerializer[0]);
    //  @formatter:on

    private int index = 0;

    @Override
    public byte[] write(List<Key> keys) {
        return serializers[index].write(keys);
    }

    @Override
    public List<Key> read(Key workKey, byte[] data) {
        try (Input input = new Input(data)) {
            byte version = serializers[0].readVersion(input);
            switch (version) {
                case VERSION_ONE:
                    index = 0;
                    break;
                case VERSION_TWO:
                    index = 1;
                    break;
                case VERSION_THREE:
                    index = 2;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported key serializer version " + version);
            }

            return serializers[index].readPayload(input, workKey);
        }
    }

    public void setIndex(int index) {
        Preconditions.checkArgument(index > 0, "version index was " + index + " but cannot be negative");
        Preconditions.checkArgument(index <= serializers.length, "version index was " + index + " but must be less than or equal to " + serializers.length);
        // convert from logical one, two, three to zero-based array
        this.index = (index - 1);
    }
}
