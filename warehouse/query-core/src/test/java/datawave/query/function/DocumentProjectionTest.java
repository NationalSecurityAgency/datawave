package datawave.query.function;

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
import datawave.query.predicate.Projection;

public class DocumentProjectionTest {

    private final ColumnVisibility cv = new ColumnVisibility("PUBLIC");
    private Document d;

    @Before
    public void setup() {
        d = new Document();
        d.put("FOO", new Content("foofighter", new Key("row", "dt\0uid", "", cv, -1), true));
        d.put("ID", new Numeric(123, new Key("row", "dt\0uid", "", cv, -1), true));

        Document primes = new Document();
        primes.put("PRIME", new Numeric(2, new Key("row", "dt\0uid", "", cv, -1), true));
        primes.put("PRIME", new Numeric(3, new Key("row", "dt\0uid", "", cv, -1), true));
        primes.put("PRIME", new Numeric(5, new Key("row", "dt\0uid", "", cv, -1), true));
        primes.put("PRIME", new Numeric(7, new Key("row", "dt\0uid", "", cv, -1), true));
        primes.put("PRIME", new Numeric(11, new Key("row", "dt\0uid", "", cv, -1), true));
        d.put("PRIMES", primes);

        Attributes others = new Attributes(true);

        Document sub1 = new Document();
        sub1.put("FOO.1", new Content("bar", new Key("row", "dt\0uid", "", cv, -1), true));
        sub1.put("ID.1", new Numeric(456, new Key("row", "dt\0uid", "", cv, -1), true));
        others.add(sub1);

        Document sub2 = new Document();
        sub2.put("FOO.2", new Content("baz", new Key("row", "dt\0uid", "", cv, -1), true));
        sub2.put("ID.2", new Numeric(789, new Key("row", "dt\0uid", "", cv, -1), true));
        others.add(sub2);

        d.put("OTHERS", others); // others' attributes have grouping context
    }

    @Test
    public void testIncludesSingleField() {
        Set<String> includes = Sets.newHashSet("OTHERS");
        DocumentProjection projection = new DocumentProjection(includes, Projection.ProjectionType.INCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(4, result.getValue().size());
    }

    @Test
    public void testIncludesTwoFields() {
        Set<String> includes = Sets.newHashSet("FOO", "ID");
        DocumentProjection projection = new DocumentProjection(includes, Projection.ProjectionType.INCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(6, result.getValue().size());
    }

    @Test
    public void testIncludesNoFieldsSpecified() {
        DocumentProjection projection = new DocumentProjection(Collections.emptySet(), Projection.ProjectionType.INCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Test
    public void testIncludesAllFields() {
        Set<String> includes = Sets.newHashSet("FOO", "ID", "PRIMES", "PRIME", "CHILDREN");
        DocumentProjection projection = new DocumentProjection(includes, Projection.ProjectionType.INCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(11, result.getValue().size());
    }

    // even though the sub-document field is not on the includes list, all the fields in the
    // sub-document are. Therefore, we keep the child document.
    @Test
    public void testIncludesAllFieldsExceptNestedDocumentFields() {
        Set<String> includes = Sets.newHashSet("FOO", "ID", "PRIME");
        DocumentProjection projection = new DocumentProjection(includes, Projection.ProjectionType.INCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(11, result.getValue().size());
    }

    @Test
    public void testExcludeSingleField() {
        Set<String> excludes = Sets.newHashSet("ID");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(8, result.getValue().size());
    }

    @Test
    public void testExcludeChildDocumentField() {
        Set<String> excludes = Sets.newHashSet("CHILDREN");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(11, result.getValue().size());
    }

    @Test
    public void testExcludeAllFields() {
        Set<String> excludes = Sets.newHashSet("FOO", "ID", "PRIMES", "PRIME", "CHILDREN");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Test
    public void testExcludeNestedField() {
        Set<String> excludes = Sets.newHashSet("PRIME");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(6, result.getValue().size());
    }

    @Test
    public void testConfirmFieldExcluded() {
        Set<String> excludes = Sets.newHashSet("PRIMES");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(6, result.getValue().size());
        assertFalse(result.getValue().containsKey("PRIMES")); // key no longer exists
    }

    @Test
    public void testConfirmGroupingContext() {
        Set<String> excludes = Sets.newHashSet("FOO");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(11, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(8, result.getValue().size());
        assertFalse(result.getValue().containsKey("FOO")); // key no longer exists
    }

    @Test
    public void testIncludesExampleCase() {
        Document d = buildExampleDocument();

        DocumentProjection projection = new DocumentProjection(Collections.singleton("NAME"), Projection.ProjectionType.INCLUDES);

        assertEquals(6, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(3, result.getValue().size());
        assertTrue(result.getValue().containsKey("NAME"));
    }

    @Test
    public void testExcludesExampleCase() {
        Document d = buildExampleDocument();

        DocumentProjection projection = new DocumentProjection(Collections.singleton("NAME"), Projection.ProjectionType.EXCLUDES);

        assertEquals(6, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(3, result.getValue().size());
        assertFalse(result.getValue().containsKey("NAME"));
    }

    private Document buildExampleDocument() {
        Document d = new Document();
        d.put("NAME", new Content("bob", new Key("row", "dt\0uid", "", cv, -1), true));
        d.put("AGE", new Numeric(40, new Key("row", "dt\0uid", "", cv, -1), true));

        Attributes children = new Attributes(true);

        Document frank = new Document();
        frank.put("NAME", new Content("frank", new Key("row", "dt\0uid", "", cv, -1), true));
        frank.put("AGE", new Numeric(12, new Key("row", "dt\0uid", "", cv, -1), true));
        children.add(frank);

        Document sally = new Document();
        sally.put("NAME", new Content("sally", new Key("row", "dt\0uid", "", cv, -1), true));
        sally.put("AGE", new Numeric(10, new Key("row", "dt\0uid", "", cv, -1), true));
        children.add(sally);

        d.put("CHILDREN", children); // others' attributes have grouping context
        return d;
    }

}
