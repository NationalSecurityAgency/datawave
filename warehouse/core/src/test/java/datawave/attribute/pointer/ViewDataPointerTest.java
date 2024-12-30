package datawave.attribute.pointer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ViewDataPointerTest {
    private ObjectMapper objectMapper;
    private ViewDataPointer pointer;

    @Before
    public void setup() {
        objectMapper = new ObjectMapper();
    }

    @Test
    public void serializeTest() throws JsonProcessingException {
        pointer = new ViewDataPointer("some_shard", "someDataType", "1.2.3", "pointerField");
        String json = objectMapper.writeValueAsString(pointer);

        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        assertEquals("dView", jsonObject.get("type").getAsString());
        assertEquals("some_shard", jsonObject.get("shard").getAsString());
        assertEquals("someDataType", jsonObject.get("dataType").getAsString());
        assertEquals("1.2.3", jsonObject.get("uid").getAsString());
        assertEquals("pointerField", jsonObject.get("view").getAsString());
        assertEquals(5, jsonObject.entrySet().size());
    }

    @Test
    public void deserializeTest() throws JsonProcessingException {
        pointer = new ViewDataPointer("some_shard", "someDataType", "1.2.3", "pointerField");
        String json = objectMapper.writeValueAsString(pointer);

        DataPointer pointer = objectMapper.readerFor(DataPointer.class).readValue(json);
        assertNotNull(pointer);
        assertTrue(pointer instanceof ViewDataPointer);
        ViewDataPointer viewPointer = (ViewDataPointer) pointer;
        assertEquals("some_shard", viewPointer.getShard());
        assertEquals("someDataType", viewPointer.getDataType());
        assertEquals("1.2.3", viewPointer.getUid());
        assertEquals("pointerField", viewPointer.getView());
    }
}
