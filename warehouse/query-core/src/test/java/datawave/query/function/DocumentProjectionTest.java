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
    private int ALL_FIELDS;

    @Before
    public void setup() {
        d = new Document();
        d.put("FOO", new Content("foofighter", new Key("row", "dt\0uid", "", cv, -1), true));
        d.put("ID", new Numeric(123, new Key("row", "dt\0uid", "", cv, -1), true));

        Attributes primes = new Attributes(true);
        primes.add(new Numeric(2, new Key("row", "dt\0uid", "", cv, -1), true));
        primes.add(new Numeric(3, new Key("row", "dt\0uid", "", cv, -1), true));
        primes.add(new Numeric(5, new Key("row", "dt\0uid", "", cv, -1), true));
        primes.add(new Numeric(7, new Key("row", "dt\0uid", "", cv, -1), true));
        primes.add(new Numeric(11, new Key("row", "dt\0uid", "", cv, -1), true));
        d.put("PRIMES", primes);

        d.put("FOO.1", new Content("bar", new Key("row", "dt\0uid", "", cv, -1), true), true, false);
        d.put("ID.1", new Numeric(456, new Key("row", "dt\0uid", "", cv, -1), true), true, false);

        d.put("FOO.2", new Content("baz", new Key("row", "dt\0uid", "", cv, -1), true), true, false);
        d.put("ID.2", new Numeric(789, new Key("row", "dt\0uid", "", cv, -1), true), true, false);

        d.put("PRIMARY_ID", new Numeric(789, new Key("row", "dt\0uid", "", cv, -1), true), true, false);

        ALL_FIELDS = d.getDictionary().size() + primes.size() - 1;
    }

    @Deprecated
    @Test
    public void testIncludesMissingSingleFieldDeprecated() {
        Set<String> includes = Sets.newHashSet("OTHERS");
        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setIncludes(includes);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Test
    public void testIncludesMissingSingleField() {
        Set<String> includes = Sets.newHashSet("OTHERS");
        DocumentProjection projection = new DocumentProjection(includes, Projection.ProjectionType.INCLUDES);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Deprecated
    @Test
    public void testIncludesTwoFieldsDeprecated() {
        Set<String> includes = Sets.newHashSet("FOO", "ID");
        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setIncludes(includes);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(6, result.getValue().size());
    }

    @Test
    public void testIncludesTwoFields() {
        Set<String> includes = Sets.newHashSet("FOO", "ID");
        DocumentProjection projection = new DocumentProjection(includes, Projection.ProjectionType.INCLUDES);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(6, result.getValue().size());
    }

    @Deprecated
    @Test
    public void testIncludesNoFieldsSpecifiedDeprecated() {
        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setIncludes(Collections.emptySet());

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Test
    public void testIncludesNoFieldsSpecified() {
        DocumentProjection projection = new DocumentProjection(Collections.emptySet(), Projection.ProjectionType.INCLUDES);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Deprecated
    @Test
    public void testIncludesAllFieldsDeprecated() {
        Set<String> includes = Sets.newHashSet("FOO", "ID", "PRIMES", "PRIMARY_ID");
        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setIncludes(includes);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(ALL_FIELDS, result.getValue().size());
    }

    @Test
    public void testIncludesAllFields() {
        Set<String> includes = Sets.newHashSet("FOO", "ID", "PRIMES", "PRIMARY_ID");
        DocumentProjection projection = new DocumentProjection(includes, Projection.ProjectionType.INCLUDES);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(ALL_FIELDS, result.getValue().size());
    }

    @Deprecated
    @Test
    public void testExcludeSingleFieldDeprecated() {
        Set<String> excludes = Sets.newHashSet("ID");
        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setExcludes(excludes);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(9, result.getValue().size());
    }

    @Test
    public void testExcludeSingleField() {
        Set<String> excludes = Sets.newHashSet("ID");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(9, result.getValue().size());
    }

    @Deprecated
    @Test
    public void testExcludeAllFieldsDeprecated() {
        Set<String> excludes = Sets.newHashSet("FOO", "ID", "PRIMES", "PRIMARY_ID");
        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setExcludes(excludes);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Test
    public void testExcludeAllFields() {
        Set<String> excludes = Sets.newHashSet("FOO", "ID", "PRIMES", "PRIMARY_ID");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(0, result.getValue().size());
    }

    @Deprecated
    @Test
    public void testConfirmFieldExcludedDeprecated() {
        Set<String> excludes = Sets.newHashSet("PRIMES");
        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setExcludes(excludes);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(7, result.getValue().size());
        assertFalse(result.getValue().containsKey("PRIMES")); // key no longer exists
    }

    @Test
    public void testConfirmFieldExcluded() {
        Set<String> excludes = Sets.newHashSet("PRIMES");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(7, result.getValue().size());
        assertFalse(result.getValue().containsKey("PRIMES")); // key no longer exists
    }

    @Deprecated
    @Test
    public void testConfirmGroupingContextDeprecated() {
        Set<String> excludes = Sets.newHashSet("FOO");
        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setExcludes(excludes);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(ALL_FIELDS - 3, result.getValue().size());
        assertFalse(result.getValue().containsKey("FOO")); // key no longer exists
    }

    @Test
    public void testConfirmGroupingContext() {
        Set<String> excludes = Sets.newHashSet("FOO");
        DocumentProjection projection = new DocumentProjection(excludes, Projection.ProjectionType.EXCLUDES);

        assertEquals(ALL_FIELDS, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(ALL_FIELDS - 3, result.getValue().size());
        assertFalse(result.getValue().containsKey("FOO")); // key no longer exists
    }

    @Deprecated
    @Test
    public void testIncludesExampleCaseDepricated() {
        Document d = buildExampleDocument();

        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setIncludes(Collections.singleton("NAME"));

        assertEquals(6, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(3, result.getValue().size());
        assertTrue(result.getValue().containsKey("NAME"));
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

    @Deprecated
    @Test
    public void testExcludesExampleCaseDeprecated() {
        Document d = buildExampleDocument();

        DocumentProjection projection = new DocumentProjection(new Projection());
        projection.setExcludes(Collections.singleton("NAME"));

        assertEquals(6, d.size());
        Map.Entry<Key,Document> result = projection.apply(Maps.immutableEntry(new Key(), d));
        assertEquals(3, result.getValue().size());
        assertFalse(result.getValue().containsKey("NAME"));
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

        d.put("NAME.1", new Content("frank", new Key("row", "dt\0uid", "", cv, -1), true), true, false);
        d.put("AGE.1", new Numeric(ALL_FIELDS, new Key("row", "dt\0uid", "", cv, -1), true), true, false);

        d.put("NAME.2", new Content("sally", new Key("row", "dt\0uid", "", cv, -1), true), true, false);
        d.put("AGE.2", new Numeric(10, new Key("row", "dt\0uid", "", cv, -1), true), true, false);

        return d;
    }

}
