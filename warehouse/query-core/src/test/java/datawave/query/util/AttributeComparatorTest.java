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
    private final Text row = new Text("20000101_12345");
    private final Text cf = new Text(); // empty column family
    private final Text cq = new Text(); // empty column qualifier
    private final ColumnVisibility cv = new ColumnVisibility("ALL");
    private final long ts = 946684800000L;

    private final Key testKeyFoo = new Key(row, new Text("foo%00;d8zay2.-3pnndm.-anolok"), cq, cv, ts);
    private final Key testKeyBar = new Key(row, new Text("bar%00;d8zay2.-3pnndm.-anolok"), cq, cv, ts);
    private final Key testKeyBaz = new Key(row, new Text("baz%00;d8zay2.-3pnndm.-anolok"), cq, cv, ts);
    private final Key testKeyIp = new Key(new Text("192.168.1.1"), new Text("someText"), cq, cv, ts);
    private final Key testKeyEmpty = new Key(row, cf, cq, cv, ts);

    private final Content content1 = new Content("foo", testKeyEmpty, true);
    private final Content content2 = new Content("bar", testKeyEmpty, true);
    private final Content content3 = new Content("baz", testKeyEmpty, true);

    private final Content combinedContent1 = new Content("foo", testKeyFoo, true);
    private final Content combinedContent2 = new Content("bar", testKeyBar, true);
    private final Content combinedContent3 = new Content("baz", testKeyBaz, true);

    private final TypeMetadata typeMetadata = new TypeMetadata();
    private final PreNormalizedAttributeFactory preNormFactory = new PreNormalizedAttributeFactory(typeMetadata);

    private final PreNormalizedAttribute preNorm1 = (PreNormalizedAttribute) preNormFactory.create("", "foo", testKeyFoo, false);
    private final PreNormalizedAttribute preNorm2 = (PreNormalizedAttribute) preNormFactory.create("", "bar", testKeyBar, false);
    private final PreNormalizedAttribute preNorm3 = (PreNormalizedAttribute) preNormFactory.create("", "baz", testKeyBaz, false);

    private final IpAddress ipAddr = new IpAddress("192.168.1.1", testKeyIp, true);

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
        HashSet<Attribute<? extends Comparable<?>>> expected = Sets.newHashSet();
        expected.add(combinedContent1);

        assertEquals(expected, AttributeComparator.combineSingleAttributes(preNorm1, content1, true));
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

    @Test
    public void testTldCase() {
        Key k1 = new Key(row, new Text("datatype%00;d8zay2.-3pnndm.-anolok"), cq, cv, ts);
        Key k2 = new Key(row, new Text("datatype%00;d8zay2.-3pnndm.-anolok.1"), cq, cv, ts);
        Key k3 = new Key(row, new Text("datatype%00;d8zay2.-3pnndm.-anolok.2"), cq, cv, ts);

        Content content1 = new Content("foo", k1, true);
        Content content2 = new Content("foo", k2, true);
        Content content3 = new Content("bar", k3, true);

        // parent/child relationship
        Set<Attribute<? extends Comparable<?>>> mergedSet1 = AttributeComparator.combineSingleAttributes(content1, content2, true);
        // multiple children relationship
        Set<Attribute<? extends Comparable<?>>> mergedSet2 = AttributeComparator.combineSingleAttributes(content2, content3, true);

        assertEquals(2, mergedSet1.size());
        assertEquals(2, mergedSet2.size());
    }

}
