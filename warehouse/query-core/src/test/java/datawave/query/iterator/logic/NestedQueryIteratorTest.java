package datawave.query.iterator.logic;

public interface NestedQueryIteratorTest {
    
    void testElementInLeaves();
    
    void testElementInChildren();
    
    void testEmptyRange();
    
    void testScanMinorRange();
    
    void testScanMinorRangeTLD();
    
    void testScanPartialRanges();
    
    void testScanPartialRangesTLD();
    
    void testScanFullRange();
    
    void testScanFullRangeTLD();
    
    void testEndingFieldMismatch();
    
    void testScanFullRangeExclusive();
    
    void testScanFullRangeExclusiveTLD();
    
    void testScanFullRangeExclusiveEventDataQueryExpressionFilter();
    
}
