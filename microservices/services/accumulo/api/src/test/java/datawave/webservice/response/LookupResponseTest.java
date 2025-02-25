package datawave.webservice.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;

import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.response.objects.DefaultKey;
import datawave.webservice.response.objects.Entry;
import datawave.webservice.response.objects.KeyBase;

public class LookupResponseTest {
    
    private LookupResponse lookupResponse;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    public void setup() {
        objectMapperSetup();
        responseSetup();
    }
    
    private void responseSetup() {
        lookupResponse = new LookupResponse();
        List<Entry> entryList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<String,String> markings = new HashMap<>();
            markings.put("columnVisibility", "A&B&C" + i);
            KeyBase responseKey = new DefaultKey();
            responseKey.setRow("row" + i);
            responseKey.setColFam("cf" + i);
            responseKey.setColQual("cq" + i);
            responseKey.setMarkings(markings);
            responseKey.setTimestamp(i);
            entryList.add(new Entry(responseKey, ByteBuffer.allocate(4).putInt(i).array()));
            lookupResponse.addException(new Exception("Exception message " + i));
            lookupResponse.addMessage("Message " + i);
        }
        lookupResponse.setEntries(entryList);
        lookupResponse.setHasResults(true);
        lookupResponse.setOperationTimeMS(10);
    }
    
    private void objectMapperSetup() {
        objectMapper = new ObjectMapper();
        // Map LookupResponse's abstract KeyBase field to concrete impl
        SimpleModule module = new SimpleModule("keybase", Version.unknownVersion());
        module.addAbstractTypeMapping(KeyBase.class, DefaultKey.class);
        objectMapper.registerModule(module);
        // For testing json deserialization via LR's jaxb annotations
        objectMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
    }
    
    @Test
    public void testLookupResponseSerDe() throws IOException {
        String ser = objectMapper.writeValueAsString(lookupResponse);
        LookupResponse de = objectMapper.readValue(ser, LookupResponse.class);
        for (int i = 0; i < 3; i++) {
            final int fi = i; // need final var for lambda
            //@formatter:off
            assertEquals(1, de.getEntries().stream().filter(
                e -> e.getKey().getRow().equals("row" + fi)
                  && e.getKey().getColFam().equals("cf" + fi)
                  && e.getKey().getColQual().equals("cq" + fi)
                  && e.getKey().getTimestamp() == fi
                  && ((DefaultKey) e.getKey()).getColumnVisibility().equals("A&B&C" + fi)
                  && ByteBuffer.wrap((byte[])e.getValue()).getInt() == fi).count());

            assertEquals(1, de.getExceptions().stream().map(QueryExceptionType::getMessage).filter(
                str -> str.equals("Exception message " + fi)).count());
            //@formatter:on
            
            assertTrue(de.getMessages().contains("Message " + fi));
        }
        assertEquals(lookupResponse.getHasResults(), de.getHasResults());
        assertEquals(lookupResponse.getOperationTimeMS(), de.getOperationTimeMS());
    }
}
