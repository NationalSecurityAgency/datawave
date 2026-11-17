package datawave.query.language.parser.jexl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.data.UUIDType;
import datawave.query.language.parser.ParseException;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.tree.ServerHeadNode;

public class TestLuceneToJexlUUIDQueryParser {
    private LuceneToJexlUUIDQueryParser parser;
    private String[] UUID_FIELDS = {"UUID", "UUID_TWO", "UUID_THREE"};

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
        assertEquals("UUID == '4361e029-99df-429d-9a70-51f7c768098b'", parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b"));
    }

    @Test
    public void testMultipleUUIDs() throws ParseException {
        assertEquals("UUID == '4361e029-99df-429d-9a70-51f7c768098b' || UUID_TWO == '6ea02cb3-644c-4c2e-9739-76322dfb477b' || UUID_THREE == '13383f57-45dc-4709-934a-363117e7c473'",
                        parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b  OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473"));
        // Technically legal, but the use case is odd
        assertEquals("UUID == '4361e029-99df-429d-9a70-51f7c768098b' && UUID_TWO == '6ea02cb3-644c-4c2e-9739-76322dfb477b' || UUID_THREE == '13383f57-45dc-4709-934a-363117e7c473'",
                        parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b AND UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b  OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473"));
    }

    @Test
    public void testNonUUIDField() throws ParseException {
        assertThrows(ParseException.class, () -> parseQuery("NOT_UUID_FIELD:4361e029-99df-429d-9a70-51f7c768098b"));

    }

    @Test
    public void testUnfielded() throws ParseException {
        assertThrows(ParseException.class, () -> parseQuery("4361e029-99df-429d-9a70-51f7c768098b"));

    }

    @Test
    public void testWildcarded() throws ParseException {
        assertThrows(ParseException.class, () -> parseQuery("UUID:4361e029-99df-429d-9a70-51f7c768098b*"));

    }

    @Test
    public void testSomeUnfielded() throws ParseException {
        assertThrows(ParseException.class, () -> parseQuery(
                        "UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR 13383f57-45dc-4709-934a-363117e7c473"));

    }

    @Test
    public void testSomeWildcarded() throws ParseException {
        assertThrows(ParseException.class, () -> parseQuery(
                        "UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473?"));

    }

    @Test
    public void testRange() throws ParseException {
        assertThrows(ParseException.class, () -> parseQuery(
                        "UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473[56 TO 60]"));

    }

    @Test
    public void testRange2() throws ParseException {
        assertThrows(ParseException.class, () -> parseQuery(
                        "UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473[56 TO 60}"));

    }

    @Test
    public void testRange3() throws ParseException {
        assertThrows(ParseException.class, () -> parseQuery(
                        "UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473{56 TO 60}"));

    }

    @Test
    public void testRange4() throws ParseException {
        assertThrows(ParseException.class, () -> parseQuery(
                        "UUID:4361e029-99df-429d-9a70-51f7c768098b OR UUID_TWO:6ea02cb3-644c-4c2e-9739-76322dfb477b OR UUID_THREE:13383f57-45dc-4709-934a-363117e7c473{56 TO 60]"));

    }
}
