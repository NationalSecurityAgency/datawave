package datawave.iterators.filter.ageoff;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataTypeParserTest {
    
    private static final boolean IS_INDEX_TABLE = true;
    private static final boolean IS_SHARD_TABLE = false;
    
    @Test
    public void usesColQualSecondTokenInIndex() {
        assertForColQual("abc", "0123456789\000abc", IS_INDEX_TABLE);
        // additional call not impacted by prior calls
        assertForColQual("cq", "0123456789_1\000cq", IS_INDEX_TABLE);
        assertForColQual("x", "0123456789\000x", IS_INDEX_TABLE);
    }
    
    @Test
    public void toleratesEmptyDataTypeInIndex() {
        assertForColQual("", "0123456789\000", IS_INDEX_TABLE);
    }
    
    @Test
    public void usesAllDataAfterFirstNullByteInIndex() {
        assertForColQual("abc\000123", "0123456789\000abc\000123", IS_INDEX_TABLE);
    }
    
    @Test
    public void nullWhenShardTooShortInIndex() {
        assertForColQual(null, "tooshort\000cq", IS_INDEX_TABLE);
    }
    
    @Test
    public void nullWhenMissingNullByteInIndex() {
        assertForColQual(null, "0123456789_cq", IS_INDEX_TABLE);
    }
    
    @Test
    public void nullWhenEmptyColQualInIndex() {
        assertForColQual(null, "", IS_INDEX_TABLE);
    }
    
    @Test
    public void usesColQualFirstTokenInDoc() {
        assertForColumnData("abc", "d", "abc\00001234567890", IS_SHARD_TABLE);
        // additional call not impacted by prior calls
        assertForColumnData("cq", "d", "cq\0000123456789_1", IS_SHARD_TABLE);
        assertForColumnData("x", "d", "x\0000123456789", IS_SHARD_TABLE);
    }
    
    @Test
    public void usesColQualFirstTokenOfManyInDoc() {
        assertForColumnData("abc", "d", "abc\0000123456789\000123", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenEmptyTokenInDoc() {
        assertForColumnData(null, "d", "\000\000", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenEmptyColQualInDoc() {
        assertForColumnData(null, "d", "", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenSingleCharInDoc() {
        assertForColumnData(null, "d", "0", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenMissingNullBytesInDoc() {
        assertForColumnData(null, "d", "0123456789_cq", IS_SHARD_TABLE);
    }
    
    @Test
    public void usesColQualFirstTokenInTf() {
        assertForColumnData("abc", "tf", "abc\00001234567890", IS_SHARD_TABLE);
        // additional call not impacted by prior calls
        assertForColumnData("cq", "tf", "cq\0000123456789_1", IS_SHARD_TABLE);
        assertForColumnData("x", "tf", "x\0000123456789", IS_SHARD_TABLE);
    }
    
    @Test
    public void usesColQualFirstTokenOfManyInTf() {
        assertForColumnData("abc", "tf", "abc\0000123456789\000123", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenEmptyTokenInTf() {
        assertForColumnData(null, "tf", "\000\000", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenEmptyColQualInTf() {
        assertForColumnData(null, "tf", "", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenSingleCharInTf() {
        assertForColumnData(null, "tf", "0", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenMissingNullBytesInTf() {
        assertForColumnData(null, "tf", "0123456789_cq", IS_SHARD_TABLE);
    }
    
    @Test
    public void usesColQualSecondOfThreeTokensInFi() {
        assertForColumnData("abc", "fi\000ignore", "val\000abc\00001234567890", IS_SHARD_TABLE);
        // additional call not impacted by prior calls
        assertForColumnData("cq", "fi\000ignore", "val\000cq\0000123456789_1", IS_SHARD_TABLE);
        assertForColumnData("x", "fi\000ignore", "val\000x\0000123456789", IS_SHARD_TABLE);
    }
    
    @Test
    public void usesSecondToLastTokenWhenExtraNullByteInFi() {
        assertForColumnData("abc", "fi\000ignore", "val\000xyz\000abc\000123", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenMissingBothNullBytesInFi() {
        assertForColumnData(null, "fi\000ignore", "", IS_SHARD_TABLE);
        assertForColumnData(null, "fi\000ignore", "val", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenMissingOneNullByteInFi() {
        assertForColumnData(null, "fi\000ignore", "\000", IS_SHARD_TABLE);
        assertForColumnData(null, "fi\000ignore", "val\000", IS_SHARD_TABLE);
        assertForColumnData(null, "fi\000ignore", "val\000ue", IS_SHARD_TABLE);
    }
    
    @Test
    public void toleratesEmptyTokenInFi() {
        assertForColumnData("a", "fi\000ignore", "\000a\000b", IS_SHARD_TABLE);
        assertForColumnData("", "fi\000ignore", "\000\000", IS_SHARD_TABLE);
        assertForColumnData("", "fi\000ignore", "\000\000\000\000", IS_SHARD_TABLE);
        assertForColumnData("", "fi\000ignore", "\000\000\000\000\000", IS_SHARD_TABLE);
    }
    
    @Test
    public void usesColFamFirstTokenInEventKey() {
        assertForColumnData("abc", "abc\0000123456789", "", IS_SHARD_TABLE);
        // additional call not impacted by prior calls
        assertForColumnData("cq", "cq\000123", "", IS_SHARD_TABLE);
        assertForColumnData("x", "x\0001", "", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenEmptyTokenInEventKey() {
        assertForColumnData(null, "\0000123456789", "", IS_SHARD_TABLE);
    }
    
    @Test
    public void usesOnlyFirstTokenWhenMultipleNullBytesInEventKey() {
        assertForColumnData("abc", "abc\0000123456789\000123", "", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenEmptyColFamInEventKey() {
        assertForColumnData(null, "", "", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenOnlyNullByteInEventKey() {
        assertForColumnData(null, "\000", "", IS_SHARD_TABLE);
    }
    
    @Test
    public void nullWhenMissingNullByteInEventKey() {
        assertForColumnData(null, "0123456789_cq", "", IS_SHARD_TABLE);
    }
    
    private void assertForColQual(String expectedDataType, String cq, boolean isIndexTable) {
        assertForColumnData(expectedDataType, "cf", cq, isIndexTable);
    }
    
    private void assertForColumnData(String expectedDataType, String cf, String cq, boolean isIndexTable) {
        ByteSequence expectedResult = (null == expectedDataType) ? null : new ArrayByteSequence(expectedDataType);
        
        Key key = new Key("row", cf, cq);
        ByteSequence actual = DataTypeParser.parseKey(key, isIndexTable);
        
        assertEquals(expectedResult, actual);
    }
}
