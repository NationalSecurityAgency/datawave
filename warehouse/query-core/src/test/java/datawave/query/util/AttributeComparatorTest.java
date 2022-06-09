package datawave.query.util;

import com.google.common.collect.Sets;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.IpAddress;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.PreNormalizedAttributeFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AttributeComparatorTest {
    
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
    
    private final Content content1 = new Content("foo", testKey4, true);
    private final Content content2 = new Content("bar", testKey5, true);
    private final Content content3 = new Content("baz", testKey6, true);
    
    private final Content combinedContent1 = new Content("foo", testKey1, true);
    private final Content combinedContent2 = new Content("bar", testKey2, true);
    private final Content combinedContent3 = new Content("baz", testKey3, true);
    
    private final TypeMetadata typeMetadata = new TypeMetadata();
    private final PreNormalizedAttributeFactory preNormFactory = new PreNormalizedAttributeFactory(typeMetadata);
    
    private final PreNormalizedAttribute preNorm1 = (PreNormalizedAttribute) preNormFactory.create("", "foo", testKey1, false);
    private final PreNormalizedAttribute preNorm2 = (PreNormalizedAttribute) preNormFactory.create("", "bar", testKey2, false);
    private final PreNormalizedAttribute preNorm3 = (PreNormalizedAttribute) preNormFactory.create("", "baz", testKey3, false);
    
    private final IpAddress ipAddr = new IpAddress("192.168.1.1", testKey7, true);
    
    private final HashSet<Attribute<? extends Comparable<?>>> contentSet = Sets.newHashSet();
    private final HashSet<Attribute<? extends Comparable<?>>> preNormSet = Sets.newHashSet();
    private final HashSet<Attribute<? extends Comparable<?>>> mixedUpSet = Sets.newHashSet();
    private final HashSet<Attribute<? extends Comparable<?>>> mergedSet = Sets.newHashSet();
    
    private Attributes contentAttributes = null;
    private Attributes preNormAttributes = null;
    private Attributes mixedUpAttributes = null;
    
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
        
        mixedUpSet.add(preNorm1);
        mixedUpSet.add(content2);
        mixedUpSet.add(ipAddr);
        mixedUpAttributes = new Attributes(mixedUpSet, true, true);
        
        mergedSet.add(combinedContent1);
        mergedSet.add(combinedContent2);
        mergedSet.add(combinedContent3);
    }
    
    @Test
    public void testSingleToSingleAttributeComparison() {
        assertTrue(AttributeComparator.singleToSingle(preNorm1, content1));
        assertTrue(AttributeComparator.singleToSingle(content1, preNorm1));
        assertFalse(AttributeComparator.singleToSingle(preNorm1, ipAddr));
    }
    
    @Test
    public void testSingleToMultipleAttributeComparison() {
        assertTrue(AttributeComparator.singleToMultiple(preNorm1, preNormAttributes));
        assertTrue(AttributeComparator.singleToMultiple(content1, preNormAttributes));
        assertFalse(AttributeComparator.singleToMultiple(ipAddr, preNormAttributes));
        
        assertTrue(AttributeComparator.singleToMultiple(content1, contentAttributes));
        assertTrue(AttributeComparator.singleToMultiple(preNorm1, contentAttributes));
        assertFalse(AttributeComparator.singleToMultiple(ipAddr, contentAttributes));
        
        assertTrue(AttributeComparator.singleToMultiple(ipAddr, mixedUpAttributes));
    }
    
    @Test
    public void testMultipleToMultipleAttributeComparison() {
        assertTrue(AttributeComparator.multipleToMultiple(preNormAttributes, contentAttributes));
        assertTrue(AttributeComparator.multipleToMultiple(contentAttributes, preNormAttributes));
        assertTrue(AttributeComparator.multipleToMultiple(preNormAttributes, mixedUpAttributes));
        assertTrue(AttributeComparator.multipleToMultiple(contentAttributes, mixedUpAttributes));
    }
    
    @Test
    public void testCombineAttributeToAttribute() {
        assertEquals(AttributeComparator.combineSingleAttributes(preNorm1, content1), combinedContent1);
    }
    
    @Test
    public void testCombineAttributeToAttributes() {
        Set<Attribute<? extends Comparable<?>>> mergedSet = AttributeComparator.combineMultipleAttributes(preNorm1, contentAttributes, true);
        assertThat(mergedSet, CoreMatchers.hasItem(combinedContent1));
        assertEquals(3, mergedSet.size());
    }
    
    @Test
    public void testCombineAttributesToAttribute() {
        Set<Attribute<? extends Comparable<?>>> mergedSet = AttributeComparator.combineMultipleAttributes(preNormAttributes, content1, true);
        assertThat(mergedSet, CoreMatchers.hasItem(combinedContent1));
        assertEquals(3, mergedSet.size());
    }
    
    @Test
    public void testCombineAttributesToAttributes() {
        Set<Attribute<? extends Comparable<?>>> mergedSet = AttributeComparator.combineMultipleAttributes(preNormAttributes, contentAttributes);
        assertThat(mergedSet, CoreMatchers.hasItem(combinedContent1));
        assertThat(mergedSet, CoreMatchers.hasItem(combinedContent2));
        assertThat(mergedSet, CoreMatchers.hasItem(combinedContent3));
        assertEquals(3, mergedSet.size());
    }
    
}
