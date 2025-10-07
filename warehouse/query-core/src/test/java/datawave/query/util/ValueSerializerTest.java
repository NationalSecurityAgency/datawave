package datawave.query.util;

import static datawave.query.util.ValueSerializerType.KRYO;
import static datawave.query.util.ValueSerializerType.WRITABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ValueSerializerTest {
    @Test
    public void testNewSerializerDefaultsWhenTypeNull() {
        ValueSerializer<?> serializerWritable = ValueSerializer.newSerializer(TestType.class, null, WRITABLE);
        assertEquals(WRITABLE, serializerWritable.getType());
    }

    @Test
    public void testNewSerializerThrowsWithUnsupportedKryo() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> ValueSerializer.newSerializer(Object.class, null, KRYO));
        assertEquals("Unsupported serializer type: KRYO for class java.lang.Object", ex.getMessage());
    }

    @ParameterizedTest
    @EnumSource(value = ValueSerializerType.class)
    public void testSerializeAndDeserialize(ValueSerializerType serializerType) throws IOException {
        TestType expected = new TestType("a", "b");
        ValueSerializer<TestType> serializer = ValueSerializer.newSerializer(TestType.class, serializerType);
        Value v1 = serializer.serialize(expected);
        TestType actual = serializer.deserialize(v1, TestType::new);

        assertEquals(expected, actual);
    }

    static class TestType implements KryoSerializable, Writable {
        private String field1;
        private String field2;

        TestType() {
            // no code
        }

        TestType(String field1, String field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof TestType))
                return false;

            TestType that = (TestType) o;
            return field1.equals(that.field1) && field2.equals(that.field2);
        }

        @Override
        public int hashCode() {
            int result = field1.hashCode();
            result = 31 * result + field2.hashCode();
            return result;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeString(field1);
            output.writeString(field2);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            this.field1 = input.readString();
            this.field2 = input.readString();
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(field1);
            dataOutput.writeUTF(field2);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.field1 = dataInput.readUTF();
            this.field2 = dataInput.readUTF();
        }
    }
}
