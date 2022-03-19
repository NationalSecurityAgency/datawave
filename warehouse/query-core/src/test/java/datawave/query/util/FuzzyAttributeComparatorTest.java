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
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FuzzyAttributeComparatorTest {

    private final Key testKey1 = new Key(new Text("20000101_69"), new Text("foo%00;d8zay2.-3pnndm.-anolok"), new Text(), new ColumnVisibility("ALL"), 946684800000L);
    private final Key testKey2 = new Key(new Text("20000101_69"), new Text("bar%00;d8zay2.-3pnndm.-anolok"), new Text(), new ColumnVisibility("ALL"), 946684800000L);
    private final Key testKey3 = new Key(new Text("20000101_69"), new Text("baz%00;d8zay2.-3pnndm.-anolok"), new Text(), new ColumnVisibility("ALL"), 946684800000L);
    private final Key testKey4 = new Key(new Text("192.168.1.1"), new Text("barf"), new Text(), new ColumnVisibility("ALL"), 946684800000L);

    private final Content content1 = new Content("foo", testKey1, true);
    private final Content content2 = new Content("bar", testKey2, true);
    private final Content content3 = new Content("baz", testKey3, true);

    private final TypeMetadata typeMetadata = new TypeMetadata();
    private final PreNormalizedAttributeFactory preNormFactory = new PreNormalizedAttributeFactory(typeMetadata);

    private final PreNormalizedAttribute preNorm1 = (PreNormalizedAttribute) preNormFactory.create("", "foo", testKey1, true);
    private final PreNormalizedAttribute preNorm2 = (PreNormalizedAttribute) preNormFactory.create("", "bar", testKey2, true);
    private final PreNormalizedAttribute preNorm3 = (PreNormalizedAttribute) preNormFactory.create("", "baz", testKey3, true);

    private final IpAddress ipAddr = new IpAddress("192.168.1.1", testKey4, true);

    private final HashSet<Attribute<? extends Comparable<?>>> contentSet = Sets.newHashSet();
    private final HashSet<Attribute<? extends Comparable<?>>> preNormSet = Sets.newHashSet();
    private final HashSet<Attribute<? extends Comparable<?>>> mixedUpSet = Sets.newHashSet();

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
    }

    @Test
    public void testSingleToSingleAttributeComparison() {
        assertTrue(FuzzyAttributeComparator.singleToSingle(preNorm1, content1));
        assertFalse(FuzzyAttributeComparator.singleToSingle(preNorm1, ipAddr));
    }

    @Test
    public void testSingleToMultipleAttributeComparison() {
        assertTrue(FuzzyAttributeComparator.singleToMultiple(preNormAttributes, preNorm1));
        assertTrue(FuzzyAttributeComparator.singleToMultiple(preNormAttributes, preNorm2));
        assertTrue(FuzzyAttributeComparator.singleToMultiple(preNormAttributes, preNorm3));
        assertFalse(FuzzyAttributeComparator.singleToMultiple(preNormAttributes, ipAddr));

        assertTrue(FuzzyAttributeComparator.singleToMultiple(contentAttributes, content1));
        assertTrue(FuzzyAttributeComparator.singleToMultiple(contentAttributes, content2));
        assertTrue(FuzzyAttributeComparator.singleToMultiple(contentAttributes, content3));
        assertFalse(FuzzyAttributeComparator.singleToMultiple(contentAttributes, ipAddr));

        assertTrue(FuzzyAttributeComparator.singleToMultiple(mixedUpAttributes, ipAddr));
    }

    @Test
    public void testMultipleToMultipleAttributeComparison() {

    }

    @Test
    public void testCombineAttributeToAttribute() {

    }

    @Test
    public void testCombineAttributeToAttributes() {

    }

    @Test
    public void testCombineAttributesToAttribute() {

    }

    @Test
    public void testCombineAttributesToAttributes() {

    }

    @Test
    public void testCombineAttributesOnlyPartialMatches() {

    }

    @Test
    public void testCombineMultipleAttributesNotToKeep() {

    }

}
