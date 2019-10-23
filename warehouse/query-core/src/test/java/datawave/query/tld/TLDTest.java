package datawave.query.tld;

import com.google.common.collect.Lists;
import datawave.query.Constants;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link TLD}.
 */
public class TLDTest {
    
    // Default values for events
    private final String row = "20190314";
    private final String field = "FIELD_A";
    private final String value = "VALUE_A";
    private final String datatype = "DATATYPE_A";
    private final ColumnVisibility cv = new ColumnVisibility("ALL");
    
    // Default values for document ids (String)
    private final String rootId = "parent.document.id";
    private final String childId = "parent.document.id.child";
    private final String grandchildId = "parent.document.id.child.grandchild";
    
    // Default values for document ids (ByteSequence)
    private final ByteSequence root = new ArrayByteSequence(rootId);
    private final ByteSequence child = new ArrayByteSequence(childId);
    private final ByteSequence grandchild = new ArrayByteSequence(grandchildId);
    
    // Build a Forward-Index Key for the Shard table
    private Key buildFiKey(String uid) {
        String cf = "fi" + '\u0000' + field;
        String cq = value + '\u0000' + datatype + '\u0000' + uid;
        return new Key(row, cf, cq);
    }
    
    // Build a Term-Frequency Key for the Shard table
    private Key buildTFKey(String uid) {
        String cf = "tf";
        String cq = datatype + '\u0000' + uid + '\u0000' + value + '\u0000' + field;
        long timestamp = 0L;
        return new Key(row, cf, cq, timestamp);
    }
    
    // Build an Event Key for the Shard table
    private Key buildEventDataKey(String uid) {
        String cf = datatype + '\u0000' + uid;
        String cq = field + '\u0000' + value;
        return new Key(row, cf, cq, cv, 0L);
    }
    
    @Test
    public void testParsePointerFromFI() {
        Key fiKey = buildFiKey(rootId);
        ByteSequence parsed = TLD.parsePointerFromFI(fiKey.getColumnQualifierData());
        ByteSequence expected = new ArrayByteSequence(datatype + '\u0000' + rootId);
        assertEquals(expected, parsed);
        
        fiKey = buildFiKey(childId);
        parsed = TLD.parsePointerFromFI(fiKey.getColumnQualifierData());
        expected = new ArrayByteSequence(datatype + '\u0000' + childId);
        assertEquals(expected, parsed);
        
        fiKey = buildFiKey(grandchildId);
        parsed = TLD.parsePointerFromFI(fiKey.getColumnQualifierData());
        expected = new ArrayByteSequence(datatype + '\u0000' + grandchildId);
        assertEquals(expected, parsed);
    }
    
    @Test
    public void testParseFieldAndValueFromFI() {
        Key fiKey = buildFiKey(rootId);
        ByteSequence parsed = TLD.parseFieldAndValueFromFI(fiKey.getColumnFamilyData(), fiKey.getColumnQualifierData());
        ByteSequence expected = new ArrayByteSequence(field + '\u0000' + value);
        assertEquals(expected, parsed);
        
        fiKey = buildFiKey(childId);
        parsed = TLD.parseFieldAndValueFromFI(fiKey.getColumnFamilyData(), fiKey.getColumnQualifierData());
        assertEquals(expected, parsed);
        
        fiKey = buildFiKey(grandchildId);
        parsed = TLD.parseFieldAndValueFromFI(fiKey.getColumnFamilyData(), fiKey.getColumnQualifierData());
        assertEquals(expected, parsed);
    }
    
    @Test
    public void testParseFieldAndValueFromTF() {
        
        // Same byte sequence is expected in each case
        ByteSequence expected = new ArrayByteSequence(field + '\u0000' + value);
        
        Key tfKey = buildTFKey(rootId);
        ByteSequence parsed = TLD.parseFieldAndValueFromTF(tfKey.getColumnQualifierData());
        assertEquals(expected, parsed);
        
        tfKey = buildTFKey(childId);
        parsed = TLD.parseFieldAndValueFromTF(tfKey.getColumnQualifierData());
        assertEquals(expected, parsed);
        
        tfKey = buildTFKey(grandchildId);
        parsed = TLD.parseFieldAndValueFromTF(tfKey.getColumnQualifierData());
        assertEquals(expected, parsed);
    }
    
    @Test
    public void testParseRootPointerFromFI() {
        Key fiKey = buildFiKey(rootId);
        ByteSequence parsed = TLD.parseRootPointerFromFI(fiKey.getColumnQualifierData());
        ByteSequence expected = new ArrayByteSequence(datatype + '\u0000' + rootId);
        assertEquals(expected, parsed);
        
        fiKey = buildFiKey(childId);
        parsed = TLD.parseRootPointerFromFI(fiKey.getColumnQualifierData());
        assertEquals(expected, parsed);
        
        fiKey = buildFiKey(grandchildId);
        parsed = TLD.parseRootPointerFromFI(fiKey.getColumnQualifierData());
        assertEquals(expected, parsed);
    }
    
    @Test
    public void testParseRootPointerFromId_String() {
        assertEquals(rootId, TLD.parseRootPointerFromId(rootId));
        assertEquals(rootId, TLD.parseRootPointerFromId(childId));
        assertEquals(rootId, TLD.parseRootPointerFromId(grandchildId));
    }
    
    @Test
    public void testParseRootPointerFromId_ByteSequence() {
        assertEquals(root, TLD.parseRootPointerFromId(root));
        assertEquals(root, TLD.parseRootPointerFromId(child));
        assertEquals(root, TLD.parseRootPointerFromId(grandchild));
    }
    
    @Test
    public void testEstimateRootPointerFromId() {
        ByteSequence root = new ArrayByteSequence(rootId);
        ByteSequence child = new ArrayByteSequence(childId);
        ByteSequence grandchild = new ArrayByteSequence(grandchildId);
        
        assertEquals(root, TLD.estimateRootPointerFromId(root));
        assertEquals(child, TLD.estimateRootPointerFromId(child));
        assertEquals(child, TLD.estimateRootPointerFromId(grandchild));
    }
    
    @Test
    public void testIsRootPointer() {
        ByteSequence root = new ArrayByteSequence(rootId);
        ByteSequence child = new ArrayByteSequence(childId);
        ByteSequence grandchild = new ArrayByteSequence(grandchildId);
        assertTrue(TLD.isRootPointer(root));
        assertFalse(TLD.isRootPointer(child));
        assertFalse(TLD.isRootPointer(grandchild));
    }
    
    @Test
    public void testFromString() {
        ByteSequence expected = new ArrayByteSequence(rootId);
        ByteSequence fromString = TLD.fromString(rootId);
        assertEquals(expected, fromString);
    }
    
    @Test
    public void testParseParentPointerFromId() {
        ByteSequence root = new ArrayByteSequence(rootId);
        ByteSequence child = new ArrayByteSequence(childId);
        ByteSequence grandchild = new ArrayByteSequence(grandchildId);
        
        assertEquals(root, TLD.parseParentPointerFromId(root));
        assertEquals(root, TLD.parseParentPointerFromId(child));
        assertEquals(root, TLD.parseParentPointerFromId(grandchild));
    }
    
    @Test
    public void testInstancesOf() {
        ByteSequence bytes = new ArrayByteSequence("how.many.dots.do.we.have?");
        ArrayList<Integer> expectedPositions = Lists.newArrayList(3, 8, 13, 16, 19);
        ArrayList<Integer> instances = TLD.instancesOf('.', bytes);
        assertEquals(expectedPositions, instances);
    }
    
    @Test
    public void testInstancesOfWithRepeat() {
        ByteSequence bytes = new ArrayByteSequence("how.many.dots.do.we.have?");
        ArrayList<Integer> expectedPositions = Lists.newArrayList(3, 8, 13);
        ArrayList<Integer> instances = TLD.instancesOf('.', bytes, 3);
        assertEquals(expectedPositions, instances);
    }
    
    @Test
    public void testLastInstancesOf() {
        ByteSequence bytes = new ArrayByteSequence("how.many.dots.do.we.have?");
        ArrayList<Integer> expectedPositions = Lists.newArrayList(19, 16, 13, 8, 3);
        ArrayList<Integer> instances = TLD.lastInstancesOf('.', bytes, -1);
        assertEquals(expectedPositions, instances);
    }
    
    @Test
    public void testLastInstancesOfWithRepeat() {
        ByteSequence bytes = new ArrayByteSequence("how.many.dots.do.we.have?");
        ArrayList<Integer> expectedPositions = Lists.newArrayList(19, 16, 13);
        ArrayList<Integer> instances = TLD.lastInstancesOf('.', bytes, 3);
        assertEquals(expectedPositions, instances);
    }
    
    @Test
    public void testInstancesOfWithNullBytes() {
        // Even indices contain a null byte
        ByteSequence bytes = new ArrayByteSequence(new byte[] {0, 1, 0, 1, 0});
        ArrayList<Integer> positions = TLD.instancesOf(0, bytes);
        assertEquals(3, positions.size());
        
        for (int ii = 0; ii < positions.size(); ii++) {
            assertEquals(ii * 2, positions.get(ii).intValue());
        }
    }
    
    // Build parent keys from FI keys
    @Test
    public void testBuildParentKey() {
        Key fiKey = buildFiKey(rootId);
        Text shard = fiKey.getRow();
        ByteSequence id = new ArrayByteSequence(rootId);
        ByteSequence cq = fiKey.getColumnQualifierData();
        Text cv = fiKey.getColumnVisibility();
        long ts = fiKey.getTimestamp();
        
        Key parentKey = TLD.buildParentKey(shard, id, cq, cv, ts);
        String cq2 = value + '\u0000' + datatype + '\u0000' + rootId;
        Key expectedKey = new Key(row, rootId, cq2, fiKey.getTimestamp());
        assertEquals(expectedKey, parentKey);
        
        // Build parent key from a child id
        id = new ArrayByteSequence(childId);
        parentKey = TLD.buildParentKey(shard, id, cq, cv, ts);
        expectedKey = new Key(row, childId, cq2, fiKey.getTimestamp());
        assertEquals(expectedKey, parentKey);
        
        // Build parent key from a grandchild id
        id = new ArrayByteSequence(grandchildId);
        parentKey = TLD.buildParentKey(shard, id, cq, cv, ts);
        expectedKey = new Key(row, grandchildId, cq2, fiKey.getTimestamp());
        assertEquals(expectedKey, parentKey);
    }
    
    // Build start keys for the next TLD
    @Test
    public void testGetNextParentKey() {
        Key parentKey = buildEventDataKey(rootId);
        Key nextParent = TLD.getNextParentKey(parentKey);
        
        String cf = datatype + '\u0000' + rootId + '.' + Constants.MAX_UNICODE_STRING;
        String cq = field + '\u0000' + value;
        Key expectedKey = new Key(row, cf, cq, cv, 0L);
        assertEquals(expectedKey, nextParent);
        
        // get next parent key from child id
        parentKey = buildEventDataKey(childId);
        nextParent = TLD.getNextParentKey(parentKey);
        assertEquals(expectedKey, nextParent);
        
        // get next parent key from grand child id
        parentKey = buildEventDataKey(grandchildId);
        nextParent = TLD.getNextParentKey(parentKey);
        assertEquals(expectedKey, nextParent);
    }
}
