package datawave.query.language.parser.lucene;

import java.util.ArrayList;
import java.util.List;

import datawave.query.data.UUIDType;
import datawave.query.language.parser.ParseException;

import org.junit.Assert;
import org.junit.Test;

public class TestLuceneUUIDQueryParser {
    @Test
    public void test1() throws ParseException {
        LuceneUUIDQueryParser queryParser = new LuceneUUIDQueryParser();
        List<UUIDType> uuidTypes = new ArrayList<>();
        uuidTypes.add(new UUIDType("FIELD", "view", 10));
        queryParser.setUuidTypes(uuidTypes);
        
        Assert.assertEquals("FIELD:0123456789*", queryParser.parse("FIELD:0123456789*").getContents());
    }
    
    @Test
    public void test2() throws ParseException {
        LuceneUUIDQueryParser queryParser = new LuceneUUIDQueryParser();
        List<UUIDType> uuidTypes = new ArrayList<>();
        uuidTypes.add(new UUIDType("FIELD", "view", 10));
        queryParser.setUuidTypes(uuidTypes);
        
        Assert.assertEquals("FIELD:0123456789*", queryParser.parse("FIELD:0123456789*").getContents());
    }
    
    @Test(expected = ParseException.class)
    public void test3() throws ParseException {
        LuceneUUIDQueryParser queryParser = new LuceneUUIDQueryParser();
        List<UUIDType> uuidTypes = new ArrayList<>();
        uuidTypes.add(new UUIDType("WRONGFIELD", "view", null));
        queryParser.setUuidTypes(uuidTypes);
        
        Assert.assertEquals("FIELD:0123456789*", queryParser.parse("FIELD:0123456789").getContents());
    }
}
