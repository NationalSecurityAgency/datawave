package datawave.webservice.common.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class MultivaluedMapDeserializerTest {

    private static final TypeReference<MultivaluedMap<String,String>> deserializationType = new TypeReference<>() {};

    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(MultivaluedMap.class, new MultivaluedMapDeserializer());
        objectMapper.registerModule(module);
    }

    @Test
    public void testDeserializeSimple() throws IOException {
        final String jsonInput = "{ \"key\": \"value\", \"foo\": \"bar\" }";
        MultivaluedMap<String,String> result = objectMapper.readValue(jsonInput, deserializationType);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("value", result.getFirst("key"));
        assertEquals("bar", result.getFirst("foo"));
    }

    @Test
    public void testDeserializeEmpty() throws IOException {
        final String jsonInput = "{}";
        MultivaluedMap<String,String> result = objectMapper.readValue(jsonInput, deserializationType);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDeserializeBadJson() {
        assertThrows(JsonParseException.class, () -> {
            final String jsonInput = "{ \"key\": \"value\", \"foo\", \"bar\" }";
            objectMapper.readValue(jsonInput, deserializationType);
        });
    }

    @Test
    public void testDeserializeList() {
        assertThrows(JsonParseException.class, () -> {
            final String jsonInput = "[\"bar\", \"baz\"]";
            objectMapper.readValue(jsonInput, deserializationType);
        });
    }

    @Test
    public void testDeserializeListValue() {
        assertThrows(JsonParseException.class, () -> {
            final String jsonInput = "{ \"key\": \"value\", \"foo\": [\"bar\", \"baz\"] }";
            objectMapper.readValue(jsonInput, deserializationType);
        });
    }

    @Test
    public void testDeserializeNumeric() throws IOException {
        assertThrows(JsonParseException.class, () -> {
            final String jsonInput = "{ \"789\": 123, \"foo\": 456 }";
            MultivaluedMap<String,String> result = objectMapper.readValue(jsonInput, deserializationType);
        });
    }
}
