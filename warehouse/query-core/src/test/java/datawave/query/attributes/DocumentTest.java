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
    private final Key testKey1 = new Key(new Text("20000101_69"), new Text("foo%00;d8zay2.-3pnndm.-anolok"), new Text(), new ColumnVisibility("ALL"),
                    946684800000L);
    private final Key testKey2 = new Key(new Text("20000101_69"), new Text("bar%00;d8zay2.-3pnndm.-anolok"), new Text(), new ColumnVisibility("ALL"),
                    946684800000L);
    private final Key testKey3 = new Key(new Text("20000101_69"), new Text("baz%00;d8zay2.-3pnndm.-anolok"), new Text(), new ColumnVisibility("ALL"),
                    946684800000L);
    private final Key testKey4 = new Key(new Text("20000101_69"), new Text("foo"), new Text(), new ColumnVisibility("ALL"), 946684800000L);
    private final Key testKey5 = new Key(new Text("20000101_69"), new Text("bar"), new Text(), new ColumnVisibility("ALL"), 946684800000L);
    private final Key testKey6 = new Key(new Text("20000101_69"), new Text("baz"), new Text(), new ColumnVisibility("ALL"), 946684800000L);
    private final Key testKey7 = new Key(new Text("192.168.1.1"), new Text("barf"), new Text(), new ColumnVisibility("ALL"), 946684800000L);
    private final Key testKey8 = new Key(new Text("foo"));
    private final Key testKey9 = new Key(new Text("lulz"));
    private final Key testKey10 = new Key(new Text("9001"));
    private final Key testKey11 = new Key(new Text("192.168.1.1"), new Text("barf"), new Text(), new ColumnVisibility("A&E"), 946684800000L);
    
    private final Content content1 = new Content("foo", testKey4, true);
    private final Content content2 = new Content("bar", testKey5, true);
    private final Content content3 = new Content("baz", testKey6, true);
    private final Content content4 = new Content("foo", testKey4, true);
    private final Content content5 = new Content("bat", testKey9, true);
    private final Content content6 = new Content("bar", testKey5, true);
    private final Content content7 = new Content("baz", testKey3, false);
    private final Content content8 = new Content("bar", testKey2, false);
    
    private final Content combinedContent1 = new Content("foo", testKey1, true);
    private final Content combinedContent2 = new Content("bar", testKey2, true);
    private final Content combinedContent3 = new Content("baz", testKey3, true);
    
    private final TypeMetadata typeMetadata = new TypeMetadata();
    private final PreNormalizedAttributeFactory preNormFactory = new PreNormalizedAttributeFactory(typeMetadata);
    
    private final PreNormalizedAttribute preNorm1 = (PreNormalizedAttribute) preNormFactory.create("", "foo", testKey1, false);
    private final PreNormalizedAttribute preNorm2 = (PreNormalizedAttribute) preNormFactory.create("", "bar", testKey2, false);
    private final PreNormalizedAttribute preNorm3 = (PreNormalizedAttribute) preNormFactory.create("", "baz", testKey3, false);
    
    private final IpAddress ipAddr1 = new IpAddress("192.168.1.1", testKey7, true);
    private final IpAddress ipAddr2 = new IpAddress("192.168.1.1", testKey11, true);
    
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
    public void testMergeWithMixedToKeep1() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", content3);
        testDoc.put("key1", content7);
        
        assertEquals(1, testDoc.get("key1").size());
        assertEquals(combinedContent3, (testDoc.get("key1")));
    }
    
    @Test
    public void testMergeWithMixedToKeep2() {
        Document testDoc = new Document(testKey8, true, true);
        
        testDoc.put("key1", content8);
        testDoc.put("key1", content2);
        
        assertEquals(1, testDoc.get("key1").size());
        assertEquals(combinedContent2, (testDoc.get("key1")));
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
