package datawave.query.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Value serializer for {@link Value} objects.
 * <p>
 * A serializer may not be thread-safe.
 * </p>
 *
 * @param <T>
 */
public interface ValueSerializer<T> {
    /**
     * Gets the type of serializer specification implemented
     *
     * @return the type of serializer
     */
    ValueSerializerType getType();

    /**
     * Serializes the item into {@link Value}
     *
     * @param item
     *            serialize into the value
     * @return a new value with the item serialized
     * @throws IOException
     */
    Value serialize(T item) throws IOException;

    /**
     * Deserializes the value into an item.
     *
     * @param value
     *            to deserialize
     * @param fn
     *            get or create an instance
     * @return the item deserialized
     * @throws IOException
     */
    T deserialize(Value value, Supplier<T> fn) throws IOException;

    /**
     * Validates that the class is supported by the serializer.
     *
     * @param clazz
     *            the class to check support
     * @return true if the class is supported, otherwise false
     */
    boolean isClassSupported(Class<T> clazz);

    /**
     * Serializes and catches any {@link IOException}, which will be rethrown as a {@link UncheckedIOException}
     *
     * @param item
     * @return
     */
    default Value serializeUnchecked(T item) {
        Value val;
        try {
            val = serialize(item);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return val;
    }

    /**
     * Deserializes and catches any {@link IOException}, which will be rethrown as a {@link UncheckedIOException}
     *
     * @param value
     * @param fn
     * @return
     */
    default T deserializeUnchecked(Value value, Supplier<T> fn) {
        T val;
        try {
            val = deserialize(value, fn);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return val;
    }

    /**
     * Creates a new serializer instance.
     * <p>
     * Note: the type of class being serialized/deserialized must meet the requirements of the serializer.
     * </p>
     *
     * @param clazz
     *            class of type to serialize
     * @param typeName
     *            type of serializer (mapped to {@link ValueSerializerType})
     * @param defaultType
     *            default type if {@code typeName} is null.
     * @return a new serializer
     * @param <V>
     */
    static <V> ValueSerializer<V> newSerializer(Class<V> clazz, String typeName, ValueSerializerType defaultType) {
        ValueSerializerType type = typeName != null ? ValueSerializerType.valueOf(typeName.toUpperCase()) : defaultType;
        return newSerializer(clazz, type);
    }

    /**
     * Creates a new serializer instance.
     * <p>
     * Note: the type of class being serialized/deserialized must meet the requirements of the serializer.
     * </p>
     *
     * @param clazz
     *            class of type to serialize
     * @param type
     *            type of serializer to create
     * @return a new serializer
     * @param <V>
     */
    static <V> ValueSerializer<V> newSerializer(Class<V> clazz, ValueSerializerType type) {
        ValueSerializer<V> serializer;
        switch (type) {
            case WRITABLE:
                // noinspection unchecked
                serializer = (ValueSerializer<V>) new WritableValueSerializer();
                break;
            case KRYO:
                // noinspection unchecked
                serializer = (ValueSerializer<V>) new KryoValueSerializer();
                break;
            default:
                throw new IllegalStateException("Unsupported serializer type: " + type);
        }
        if (!serializer.isClassSupported(clazz)) {
            throw new IllegalStateException("Unsupported serializer type: " + type + " for " + clazz);
        }
        return serializer;
    }

    class WritableValueSerializer implements ValueSerializer<Writable> {
        @Override
        public Value serialize(Writable item) {
            return new Value(WritableUtils.toByteArray(item));
        }

        @Override
        public Writable deserialize(Value value, Supplier<Writable> fn) throws IOException {
            Writable data = fn.get();
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(value.get()))) {
                data.readFields(dis);
            }
            return data;
        }

        @Override
        public boolean isClassSupported(Class<Writable> clazz) {
            return Writable.class.isAssignableFrom(clazz);
        }

        @Override
        public ValueSerializerType getType() {
            return ValueSerializerType.WRITABLE;
        }
    }

    class KryoValueSerializer implements ValueSerializer<KryoSerializable> {
        private final static int DEFAULT_BUFFER_SIZE = 4096;

        private final Kryo kryo = new Kryo();
        private final byte[] bufferInput = new byte[DEFAULT_BUFFER_SIZE];
        private final byte[] bufferOutput = new byte[DEFAULT_BUFFER_SIZE];

        @Override
        public Value serialize(KryoSerializable item) {
            try (Output output = new Output(bufferOutput)) {
                item.write(kryo, output);
                output.flush();
                return new Value(output.toBytes());
            }
        }

        @Override
        public KryoSerializable deserialize(Value value, Supplier<KryoSerializable> fn) throws IOException {
            KryoSerializable data = fn.get();
            try (ByteArrayInputStream bin = new ByteArrayInputStream(value.get()); Input input = new Input(bufferInput)) {
                input.setInputStream(bin);
                data.read(kryo, input);
            }
            return data;
        }

        @Override
        public boolean isClassSupported(Class<KryoSerializable> clazz) {
            return KryoSerializable.class.isAssignableFrom(clazz);
        }

        @Override
        public ValueSerializerType getType() {
            return ValueSerializerType.KRYO;
        }
    }
}
