package datawave.query.language.parser.lucene;

import datawave.query.data.UUIDType;
import datawave.query.language.parser.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestLuceneUUIDQueryParser {
    @Test
    public void test1() throws ParseException {
        LuceneUUIDQueryParser queryParser = new LuceneUUIDQueryParser();
        List<UUIDType> uuidTypes = new ArrayList<>();
        uuidTypes.add(new UUIDType("FIELD", "view", 10));
        queryParser.setUuidTypes(uuidTypes);
        
        Assertions.assertEquals("FIELD:0123456789*", queryParser.parse("FIELD:0123456789*").getContents());
    }
    
    @Test
    public void test2() throws ParseException {
        LuceneUUIDQueryParser queryParser = new LuceneUUIDQueryParser();
        List<UUIDType> uuidTypes = new ArrayList<>();
        uuidTypes.add(new UUIDType("FIELD", "view", 10));
        queryParser.setUuidTypes(uuidTypes);
        
        Assertions.assertEquals("FIELD:0123456789*", queryParser.parse("FIELD:0123456789*").getContents());
    }
    
    @Test
    public void test3() {
        LuceneUUIDQueryParser queryParser = new LuceneUUIDQueryParser();
        List<UUIDType> uuidTypes = new ArrayList<>();
        uuidTypes.add(new UUIDType("WRONGFIELD", "view", null));
        queryParser.setUuidTypes(uuidTypes);
        
        assertThrows(ParseException.class, () -> Assertions.assertEquals("FIELD:0123456789*", queryParser.parse("FIELD:0123456789").getContents()));
    }
}
