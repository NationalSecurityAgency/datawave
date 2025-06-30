package datawave.query.attributes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.ByteArrayOutputStream;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.type.NoOpType;

public class TypeAttributeTest extends AttributeTest {

    @Test
    public void validateSerializationOfToKeepFlag() {
        NoOpType type = new NoOpType("no op value");
        Key docKey = new Key("shard", "datatype\0uid");

        TypeAttribute<?> attr = new TypeAttribute<>(type, docKey, false);
        testToKeep(attr, false);

        attr = new TypeAttribute<>(type, docKey, true);
        testToKeep(attr, true);
    }

    @Test
    public void testExceptionLeadsToNoOpType() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);

        // note: if further changes are made to serialization this test will need to be updated
        output.writeInt(0, true);
        output.writeString("MrMcDoesn'tExist");
        output.writeBoolean(false); // write metadata when not set
        output.writeString("delegate value as string");
        output.writeBoolean(false); // to keep false
        output.writeInt(12, true); // hash code
        output.flush();

        Input input = new Input(baos.toByteArray());
        TypeAttribute<?> type = new TypeAttribute<>();
        type.read(new Kryo(), input);

        assertInstanceOf(TypeAttribute.class, type);
        assertInstanceOf(NoOpType.class, type.getType());
        assertEquals("delegate value as string", type.getData().toString());
    }
}
