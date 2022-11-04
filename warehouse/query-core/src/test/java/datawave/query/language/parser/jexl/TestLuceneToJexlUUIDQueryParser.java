package datawave.query.language.parser.jexl;

import datawave.query.data.UUIDType;
import datawave.query.language.parser.ParseException;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.tree.ServerHeadNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestLuceneToJexlUUIDQueryParser {
    private LuceneToJexlUUIDQueryParser parser;
    private final String[] UUID_FIELDS = {"UUID", "UUID_TWO", "UUID_THREE"};
    
    @BeforeEach
    public void setUp() {
        parser = new LuceneToJexlUUIDQueryParser();
        List<UUIDType> uuidTypes = new ArrayList<>();
        
        for (String s : UUID_FIELDS) {
            UUIDType uuidType = new UUIDType();
            uuidType.setFieldName(s);
            uuidTypes.add(uuidType);
        }
        parser.setUuidTypes(uuidTypes);
    }
    
    private String parseQuery(String query) throws ParseException {
        String parsedQuery = null;
        
        try {
            QueryNode node = parser.parse(query);
            if (node instanceof ServerHeadNode) {
                parsedQuery = node.getOriginalQuery();
            }
        } catch (RuntimeException e) {
            throw new ParseException(e);
        }
        return parsedQuery;
    }
    
    @Test
    public void testSingleUUID() throws ParseException {
        Assertions.assertEquals("UUID == '4361e029-99df-429d-9a70-51f7c768098b'", parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b"));
    }
    
    @Test
    public void testMultipleUUIDs() throws ParseException {
        Assertions.assertEquals(
                        "UUID == '4361e029-99df-429d-9a70-51f7c768098b' || UUID_TWO == '6ea02cb3-644c-4c2e-9739-76322dfb477b' || UUID_THREE == '13383f57-45dc-4709-934a-363117e7c473'",
                        parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b  OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473"));
        // Technically legal, but the use case is odd
        Assertions.assertEquals(
                        "UUID == '4361e029-99df-429d-9a70-51f7c768098b' && UUID_TWO == '6ea02cb3-644c-4c2e-9739-76322dfb477b' || UUID_THREE == '13383f57-45dc-4709-934a-363117e7c473'",
                        parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b AND UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b  OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473"));
    }
    
    @Test
    public void testNonUUIDField() {
        assertThrows(ParseException.class, () -> parseQuery("NOT_UUID_FIELD:4361e029-99df-429d-9a70-51f7c768098b"));
    }
    
    @Test
    public void testUnfielded() {
        assertThrows(ParseException.class, () -> parseQuery("4361e029-99df-429d-9a70-51f7c768098b"));
    }
    
    @Test
    public void testWildcarded() {
        assertThrows(ParseException.class, () -> parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b*"));
    }
    
    @Test
    public void testSomeUnfielded() {
        assertThrows(ParseException.class,
                        () -> parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR 13383f57-45dc-4709-934a-363117e7c473"));
    }
    
    @Test
    public void testSomeWildcarded() {
        assertThrows(ParseException.class,
                        () -> parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473?"));
    }
    
    @Test
    public void testRange() throws ParseException {
        assertThrows(ParseException.class,
                        () -> parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473[56 TO 60]"));
    }
    
    @Test
    public void testRange2() {
        assertThrows(ParseException.class,
                        () -> parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473[56 TO 60}"));
    }
    
    @Test
    public void testRange3() {
        assertThrows(ParseException.class,
                        () -> parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473{56 TO 60}"));
    }
    
    @Test
    public void testRange4() {
        assertThrows(ParseException.class,
                        () -> parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473{56 TO 60]"));
    }
}
