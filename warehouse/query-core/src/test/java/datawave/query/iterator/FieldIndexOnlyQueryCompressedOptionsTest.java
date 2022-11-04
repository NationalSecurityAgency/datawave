package datawave.query.iterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FieldIndexOnlyQueryCompressedOptionsTest {
    
    private FieldIndexOnlyQueryIterator mockIterator;
    private Map<String,String> optionsMap;
    private Range testRange;
    private Collection<ByteSequence> byteSeq;
    
    @BeforeEach
    public void setup() {
        
        mockIterator = new FieldIndexOnlyQueryIterator();
        optionsMap = new HashMap<>();
        optionsMap.put("query", "sample_query");
        optionsMap.put("index.only.fields", "sample_field");
        optionsMap.put("start.time", "3");
        optionsMap.put("end.time", "4");
        optionsMap.put("query.mapping.compress", "true");
        
        try {
            String compressedKey = QueryOptions.compressOption("noIndex1", QueryOptions.UTF8);
            String compressedValue = QueryOptions.compressOption("data1", QueryOptions.UTF8);
            String compressedKey2 = QueryOptions.compressOption("noIndex2", QueryOptions.UTF8);
            String compressedValue2 = QueryOptions.compressOption("data2", QueryOptions.UTF8);
            optionsMap.put("non.indexed.dataTypes", compressedKey + ":" + compressedValue + ";" + compressedKey2 + ":" + compressedValue2);
        } catch (Exception e) {
            e.getMessage();
        }
        
        mockIterator.compressedMappings = true;
        mockIterator.documentOptions = optionsMap;
        
        Key key1 = new Key("key_1");
        Key key2 = new Key("key_2");
        testRange = new Range(key1, key2);
        
        ByteSequence mockSeq = EasyMock.createMock(ByteSequence.class);
        byteSeq = new ArrayList<>();
        byteSeq.add(mockSeq);
        
    }
    
    @Test
    public void testValidOptions() {
        
        try {
            mockIterator.createAndSeekIndexIterator(testRange, byteSeq, false);
        } catch (Exception e) {
            e.getMessage();
        }
        
        Assertions.assertTrue(mockIterator.validateOptions(optionsMap));
    }
    
    @Test
    public void testHasCompressedMappings() {
        
        try {
            mockIterator.createAndSeekIndexIterator(testRange, byteSeq, false);
        } catch (Exception e) {
            e.getMessage();
        }
        
        Assertions.assertTrue(mockIterator.compressedMappings);
        
    }
    
}
