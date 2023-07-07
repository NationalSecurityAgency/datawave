package datawave.query.function;

import static datawave.query.function.LogTiming.TIMING_METADATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.query.attributes.TimingMetadata;

public class DocumentProjectionTest {

    private final ColumnVisibility cv = new ColumnVisibility("PUBLIC");
    private final Key docKey = new Key("row", "dt\0uid", "", cv, -1);
    private Document d;

    @Before
    public void setup() {
        d = new Document();
        d.put("FOO", new Content("foofighter", docKey, true));
        d.put("ID", new Numeric(123, docKey, true));

        Document primes = new Document();
        primes.put("PRIME", new Numeric(2, docKey, true));
        primes.put("PRIME", new Numeric(3, docKey, true));
        primes.put("PRIME", new Numeric(5, docKey, true));
        primes.put("PRIME", new Numeric(7, docKey, true));
        primes.put("PRIME", new Numeric(11, docKey, true));
        d.put("PRIMES", primes);

        Attributes others = new Attributes(true);

        Document sub1 = new Document();
        sub1.put("FOO.1", new Content("bar", docKey, true));
        sub1.put("ID.1", new Numeric(456, docKey, true));
        others.add(sub1);

        Document sub2 = new Document();
        sub2.put("FOO.2", new Content("baz", docKey, true));
        sub2.put("ID.2", new Numeric(789, docKey, true));
        others.add(sub2);

        d.put("OTHERS", others); // others' attributes have grouping context
    }

    @Test
    public void testIncludesSingleField() {
        Set<String> includes = Sets.newHashSet("OTHERS");
        DocumentProjection projection = new DocumentProjection();
        projection.setIncludes(includes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(4, result.getValue().size());
    }

    @Test
    public void testIncludesTwoFields() {
        Set<String> includes = Sets.newHashSet("FOO", "ID");
        DocumentProjection projection = new DocumentProjection();
        projection.setIncludes(includes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(6, result.getValue().size());
    }

    @Test
    public void testIncludesNoFieldsSpecified() {
        DocumentProjection projection = new DocumentProjection();
        projection.setIncludes(Collections.emptySet());

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Test
    public void testIncludesAllFields() {
        Set<String> includes = Sets.newHashSet("FOO", "ID", "PRIMES", "PRIME", "CHILDREN");
        DocumentProjection projection = new DocumentProjection();
        projection.setIncludes(includes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(11, result.getValue().size());
    }

    // even though the sub-document field is not on the includes list, all the fields in the
    // sub-document are. Therefore, we keep the child document.
    @Test
    public void testIncludesAllFieldsExceptNestedDocumentFields() {
        Set<String> includes = Sets.newHashSet("FOO", "ID", "PRIME");
        DocumentProjection projection = new DocumentProjection();
        projection.setIncludes(includes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(11, result.getValue().size());
    }

    @Test
    public void testExcludeSingleField() {
        Set<String> excludes = Sets.newHashSet("ID");
        DocumentProjection projection = new DocumentProjection();
        projection.setExcludes(excludes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(8, result.getValue().size());
    }

    @Test
    public void testExcludeChildDocumentField() {
        Set<String> excludes = Sets.newHashSet("CHILDREN");
        DocumentProjection projection = new DocumentProjection();
        projection.setExcludes(excludes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(11, result.getValue().size());
    }

    @Test
    public void testExcludeAllFields() {
        Set<String> excludes = Sets.newHashSet("FOO", "ID", "PRIMES", "PRIME", "CHILDREN");
        DocumentProjection projection = new DocumentProjection();
        projection.setExcludes(excludes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Test
    public void testExcludeNestedField() {
        Set<String> excludes = Sets.newHashSet("PRIME");
        DocumentProjection projection = new DocumentProjection();
        projection.setExcludes(excludes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(6, result.getValue().size());
    }

    @Test
    public void testConfirmFieldExcluded() {
        Set<String> excludes = Sets.newHashSet("PRIMES");
        DocumentProjection projection = new DocumentProjection();
        projection.setExcludes(excludes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(6, result.getValue().size());
        assertFalse(result.getValue().containsKey("PRIMES")); // key no longer exists
    }

    @Test
    public void testConfirmGroupingContext() {
        Set<String> excludes = Sets.newHashSet("FOO");
        DocumentProjection projection = new DocumentProjection();
        projection.setExcludes(excludes);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(8, result.getValue().size());
        assertFalse(result.getValue().containsKey("FOO")); // key no longer exists
    }

    @Test
    public void testIncludesExampleCase() {
        Document d = buildExampleDocument();

        DocumentProjection projection = new DocumentProjection();
        projection.setIncludes(Collections.singleton("NAME"));

        assertEquals(6, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(3, result.getValue().size());
        assertTrue(result.getValue().containsKey("NAME"));
    }

    @Test
    public void testExcludesExampleCase() {
        Document d = buildExampleDocument();

        DocumentProjection projection = new DocumentProjection();
        projection.setExcludes(Collections.singleton("NAME"));

        assertEquals(6, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(3, result.getValue().size());
        assertFalse(result.getValue().containsKey("NAME"));
    }

    @Test
    public void testIncludeWithTimingMetadata() {
        Document d = buildExampleDocument();
        d.put(TIMING_METADATA, createTimingMetadata());
        assertTrue(d.containsKey(TIMING_METADATA));
        assertTrue(d.get(TIMING_METADATA) instanceof TimingMetadata);

        DocumentProjection projection = new DocumentProjection();
        projection.setIncludes(Collections.singleton("NAME"));

        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));

        assertTrue(result.getValue().containsKey(TIMING_METADATA));
        assertTrue(result.getValue().get(TIMING_METADATA) instanceof TimingMetadata);
    }

    @Test
    public void testExcludeWithTimingMetadata() {
        Document d = buildExampleDocument();
        d.put(TIMING_METADATA, createTimingMetadata());
        assertTrue(d.containsKey(TIMING_METADATA));
        assertTrue(d.get(TIMING_METADATA) instanceof TimingMetadata);

        DocumentProjection projection = new DocumentProjection();
        projection.setExcludes(Collections.singleton("NAME"));

        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));

        assertTrue(result.getValue().containsKey(TIMING_METADATA));
        assertTrue(result.getValue().get(TIMING_METADATA) instanceof TimingMetadata);
    }

    private Document buildExampleDocument() {
        Document d = new Document();
        d.put("NAME", new Content("bob", docKey, true));
        d.put("AGE", new Numeric(40, docKey, true));

        Attributes children = new Attributes(true);

        Document frank = new Document();
        frank.put("NAME", new Content("frank", docKey, true));
        frank.put("AGE", new Numeric(12, docKey, true));
        children.add(frank);

        Document sally = new Document();
        sally.put("NAME", new Content("sally", docKey, true));
        sally.put("AGE", new Numeric(10, docKey, true));
        children.add(sally);

        d.put("CHILDREN", children); // others' attributes have grouping context
        return d;
    }

    private TimingMetadata createTimingMetadata() {
        TimingMetadata metadata = new TimingMetadata();
        metadata.setNextCount(25L);
        metadata.setSourceCount(2L);
        metadata.setSeekCount(15L);
        metadata.setYieldCount(0L);
        metadata.setHost("localhost");

        metadata.setToKeep(true);
        metadata.setFromIndex(false);

        return metadata;
    }

}
