package datawave.query.attributes;

import com.google.common.collect.Sets;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DocumentTest {
    private final Text row = new Text("20000101_12345");
    private final Text cf = new Text(); // empty column family
    private final Text cq = new Text(); // empty column qualifier
    private final ColumnVisibility cv = new ColumnVisibility("ALL");
    private final long ts = 946684800000L;
    
    private final Key testKeyFoo = new Key(row, new Text("foo%00;d8zay2.-3pnndm.-anolok"), cq, cq, ts);
    private final Key testKeyBar = new Key(row, new Text("bar%00;d8zay2.-3pnndm.-anolok"), cq, cq, ts);
    private final Key testKeyBaz = new Key(row, new Text("baz%00;d8zay2.-3pnndm.-anolok"), cq, cq, ts);
    private final Key testKeyIp = new Key(new Text("192.168.1.1"), new Text("someText"), cq, cq, ts);
    private final Key testKeyEmpty = new Key(row, cf, cq, cq, ts);
    private final Key testKey8 = new Key(new Text("foo"));
    private final Key testKey9 = new Key(new Text("someKeyText"));
    private final Key testKey10 = new Key(new Text("9001"));
    private final Key testKeyIpDifferentAuths = new Key(new Text("192.168.1.1"), new Text("someText"), cq, new ColumnVisibility("A&E"), ts);
    
    private final Content content1 = new Content("foo", testKeyEmpty, true);
    private final Content content2 = new Content("bar", testKeyEmpty, true);
    private final Content content3 = new Content("baz", testKeyEmpty, true);
    private final Content content4 = new Content("foo", testKeyEmpty, true);
    private final Content content5 = new Content("bat", testKey9, true);
    private final Content content6 = new Content("bar", testKeyEmpty, true);
    
    private final Content combinedContent1 = new Content("foo", testKeyFoo, true);
    private final Content combinedContent2 = new Content("bar", testKeyBar, true);
    private final Content combinedContent3 = new Content("baz", testKeyBaz, true);
    
    private final TypeMetadata typeMetadata = new TypeMetadata();
    private final PreNormalizedAttributeFactory preNormFactory = new PreNormalizedAttributeFactory(typeMetadata);
    
    private final PreNormalizedAttribute preNorm1 = (PreNormalizedAttribute) preNormFactory.create("", "foo", testKeyFoo, false);
    private final PreNormalizedAttribute preNorm2 = (PreNormalizedAttribute) preNormFactory.create("", "bar", testKeyBar, false);
    private final PreNormalizedAttribute preNorm3 = (PreNormalizedAttribute) preNormFactory.create("", "baz", testKeyBaz, false);
    
    private final IpAddress ipAddr1 = new IpAddress("192.168.1.1", testKeyIp, true);
    private final IpAddress ipAddr2 = new IpAddress("192.168.1.1", testKeyIpDifferentAuths, true);
    
    private final Numeric num = new Numeric("9001", testKey10, true);
    
    private final HashSet<Attribute<? extends Comparable<?>>> contentSet = Sets.newHashSet();
    private final HashSet<Attribute<? extends Comparable<?>>> preNormSet = Sets.newHashSet();
    private final HashSet<Attribute<? extends Comparable<?>>> mixedUpSet = Sets.newHashSet();
    private final HashSet<Attribute<? extends Comparable<?>>> moreContentSet = Sets.newHashSet();
    
    private Attributes contentAttributes = null;
    private Attributes preNormAttributes = null;
    private Attributes mixedUpAttributes = null;
    private Attributes moreContentAttributes = null;
    
    @Before
    public void setup() {
        contentSet.add(content1);
        contentSet.add(content2);
        contentSet.add(content3);
        contentAttributes = new Attributes(contentSet, true, true);
        
        preNormSet.add(preNorm1);
        preNormSet.add(preNorm2);
        preNormSet.add(preNorm3);
        preNormAttributes = new Attributes(preNormSet, true, true);
        
        mixedUpSet.add(content5);
        mixedUpSet.add(num);
        mixedUpSet.add(ipAddr1);
        mixedUpAttributes = new Attributes(mixedUpSet, true, true);
        
        moreContentSet.add(content4);
        moreContentSet.add(content6);
        moreContentAttributes = new Attributes(moreContentSet, true, true);
    }
    
    @Test
    public void testMergeSingleToSingleWithoutDuplicates() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", ipAddr1);
        testDoc.put("key1", content1);
        
        assertEquals(2, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(ipAddr1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content1));
    }
    
    @Test
    public void testMergeSingleToSingleWithDuplicatesSameType() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", content1);
        testDoc.put("key1", content4);
        
        assertEquals(1, testDoc.get("key1").size());
        assertEquals(content1, testDoc.get("key1"));
    }
    
    @Test
    public void testMergeSingleToSingleWithDuplicatesDifferentType() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", preNorm1);
        testDoc.put("key1", content1);
        
        assertEquals(1, testDoc.getAttributes().size());
        assertTrue(testDoc.getAttributes().contains(combinedContent1));
    }
    
    @Test
    public void testMergeSingleToMultipleWithoutDuplicates() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", contentAttributes);
        testDoc.put("key1", ipAddr1);
        
        assertEquals(4, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(ipAddr1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content2));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content3));
    }
    
    @Test
    public void testMergeSingleToMultipleWithDuplicatesSameType() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", contentAttributes);
        testDoc.put("key1", content3);
        
        assertEquals(3, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content2));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content3));
    }
    
    @Test
    public void testMergeSingleToMultipleWithDuplicatesDifferentType() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", contentAttributes);
        testDoc.put("key1", preNorm1);
        
        assertEquals(3, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(combinedContent1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content2));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content3));
    }
    
    @Test
    public void testMergeMultipleToSingleWithoutDuplicates() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", ipAddr1);
        testDoc.put("key1", contentAttributes);
        
        assertEquals(4, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(ipAddr1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content2));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content3));
    }
    
    @Test
    public void testMergeMultipleToSingleWithDuplicatesSameType() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", content3);
        testDoc.put("key1", contentAttributes);
        
        assertEquals(3, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content2));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content3));
    }
    
    @Test
    public void testMergeMultipleToSingleWithDuplicatesDifferentType() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", contentAttributes);
        testDoc.put("key1", preNorm1);
        
        assertEquals(3, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(combinedContent1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content2));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content3));
    }
    
    @Test
    public void testMergeMultiAttributesWithoutDuplicates() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", contentAttributes);
        testDoc.put("key1", mixedUpAttributes);
        
        assertEquals(6, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content2));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content3));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content5));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(num));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(ipAddr1));
    }
    
    @Test
    public void testMergeMultiAttributesWithDuplicatesSameType() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", contentAttributes);
        testDoc.put("key1", moreContentAttributes);
        
        assertEquals(3, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content2));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(content3));
    }
    
    @Test
    public void testMergeMultiAttributesWithDuplicatesDifferentType() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", contentAttributes);
        testDoc.put("key1", preNormAttributes);
        
        assertEquals(3, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(combinedContent1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(combinedContent2));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(combinedContent3));
    }
    
    @Test
    public void testMergeWithMixedKeys() {
        Document testDoc = new Document();
        
        testDoc.put("key1", ipAddr1);
        testDoc.put("key2", content1);
        
        assertEquals(1, testDoc.get("key1").size());
        assertEquals(ipAddr1, (testDoc.get("key1")));
        
        assertEquals(1, testDoc.get("key2").size());
        assertEquals(content1, (testDoc.get("key2")));
    }
    
    @Test
    public void testMergeDifferentVisibilities() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", ipAddr1);
        testDoc.put("key1", ipAddr2);
        
        assertEquals(2, testDoc.get("key1").size());
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(ipAddr1));
        assertTrue(((Attributes) testDoc.get("key1")).getAttributes().contains(ipAddr2));
    }
    
    @Test
    public void testPreferFromEvent() {
        Document testDoc = new Document(testKey8, true, true);
        
        content1.setFromIndex(true);
        preNorm1.setFromIndex(false);
        
        testDoc.put("key1", content1);
        testDoc.put("key1", preNorm1);
        
        assertEquals(1, testDoc.get("key1").size());
        assertEquals(preNorm1, testDoc.get("key1"));
    }
    
}
