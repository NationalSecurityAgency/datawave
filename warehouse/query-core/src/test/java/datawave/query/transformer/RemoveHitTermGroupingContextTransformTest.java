package datawave.query.transformer;

import static datawave.query.function.JexlEvaluation.HIT_TERM_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;

import com.google.common.collect.Maps;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;

public class RemoveHitTermGroupingContextTransformTest {

    private final ColumnVisibility cv = new ColumnVisibility("PUBLIC");
    private final Key docKey = new Key("row", "dt\0uid", "", cv, -1);

    @Test
    public void testTokenContextRemoved() {
        Document d = documentWithHitTerms("TF.123:to the park");

        apply(d);

        assertEquals(Collections.singleton("TF:to the park"), hitTerms(d));
    }

    @Test
    public void testEventGroupingContextRemoved() {
        Document d = documentWithHitTerms("URL_URL.208.2.0:someplace.com");

        apply(d);

        assertEquals(Collections.singleton("URL_URL:someplace.com"), hitTerms(d));
    }

    @Test
    public void testHitTermWithoutGroupingContextIsUntouched() {
        Document d = documentWithHitTerms("TF:to the park");
        Attribute<?> before = d.get(HIT_TERM_FIELD);

        apply(d);

        assertSame(before, d.get(HIT_TERM_FIELD));
    }

    @Test
    public void testOnlyTheFieldNameIsRewritten() {
        Document d = documentWithHitTerms("TF.123:12:34:56");

        apply(d);

        assertEquals(Collections.singleton("TF:12:34:56"), hitTerms(d));
    }

    @Test
    public void testDottedValueIsNotMistakenForGroupingContext() {
        Document d = documentWithHitTerms("TF:someplace.com");
        Attribute<?> before = d.get(HIT_TERM_FIELD);

        apply(d);

        assertSame(before, d.get(HIT_TERM_FIELD));
    }

    @Test
    public void testMixedHitTerms() {
        Document d = documentWithHitTerms("TF.123:to the park", "TF:park", "NAME.FOO.1:bob");

        apply(d);

        assertEquals(Set.of("TF:to the park", "TF:park", "NAME:bob"), hitTerms(d));
    }

    @Test
    public void testHitTermsFromSiblingContextsAreMerged() {
        Document d = documentWithHitTerms("TF.123:park", "TF.456:park");

        apply(d);

        assertEquals(Collections.singleton("TF:park"), hitTerms(d));
    }

    @Test
    public void testHitTermsWithDifferentVisibilitiesAreNotMerged() {
        ColumnVisibility other = new ColumnVisibility("PRIVATE");
        Document d = documentWithHitTerms(hitTerm("TF.123:park", cv, 1L), hitTerm("TF.456:park", other, 1L));

        apply(d);

        Set<Attribute<? extends Comparable<?>>> hitTerms = ((Attributes) d.get(HIT_TERM_FIELD)).getAttributes();
        assertEquals(2, hitTerms.size());
        Set<ColumnVisibility> visibilities = new HashSet<>();
        for (Attribute<?> attribute : hitTerms) {
            assertEquals("TF:park", ((Content) attribute).getContent());
            visibilities.add(attribute.getColumnVisibility());
        }
        assertEquals(Set.of(cv, other), visibilities);
    }

    @Test
    public void testHitTermsWithDifferentTimestampsAreNotMerged() {
        Document d = documentWithHitTerms(hitTerm("TF.123:park", cv, 1L), hitTerm("TF.456:park", cv, 2L));

        apply(d);

        Set<Attribute<? extends Comparable<?>>> hitTerms = ((Attributes) d.get(HIT_TERM_FIELD)).getAttributes();
        assertEquals(2, hitTerms.size());
        Set<Long> timestamps = new HashSet<>();
        for (Attribute<?> attribute : hitTerms) {
            assertEquals("TF:park", ((Content) attribute).getContent());
            assertEquals(cv, attribute.getColumnVisibility());
            timestamps.add(attribute.getTimestamp());
        }
        assertEquals(Set.of(1L, 2L), timestamps);
    }

    @Test
    public void testHitTermsBeforeTheFirstRewriteAreKept() {
        Document d = documentWithHitTerms("TF:park", "NAME:alice", "NAME.FOO.1:bob", "TF:lake");

        apply(d);

        assertEquals(Set.of("TF:park", "NAME:alice", "NAME:bob", "TF:lake"), hitTerms(d));
        assertEquals(4, d.get(HIT_TERM_FIELD).size());
    }

    @Test
    public void testMetadataAndVisibilityArePreserved() {
        Document d = documentWithHitTerms("TF.123:to the park");

        apply(d);

        Attributes hitTerms = (Attributes) d.get(HIT_TERM_FIELD);
        Content hitTerm = (Content) hitTerms.getAttributes().iterator().next();
        assertEquals(docKey, hitTerm.getMetadata());
        assertEquals(cv, hitTerm.getColumnVisibility());
        assertTrue(hitTerm.isToKeep());
    }

    @Test
    public void testDocumentWithoutHitTerms() {
        Document d = new Document();
        d.put("TF", new Content("park", docKey, true));

        apply(d);

        assertEquals(1, d.getDictionary().size());
        assertEquals(Collections.emptySet(), hitTerms(d));
    }

    @Test
    public void testNullEntry() {
        assertNull(new RemoveHitTermGroupingContextTransform().apply(null));
    }

    private Document documentWithHitTerms(String... terms) {
        Content[] hitTerms = new Content[terms.length];
        for (int i = 0; i < terms.length; i++) {
            hitTerms[i] = new Content(terms[i], docKey, true);
            hitTerms[i].setColumnVisibility(cv);
        }
        return documentWithHitTerms(hitTerms);
    }

    private Document documentWithHitTerms(Content... hitTerms) {
        Attributes attributes = new Attributes(true);
        for (Content hitTerm : hitTerms) {
            attributes.add(hitTerm);
        }

        Document d = new Document();
        d.put(HIT_TERM_FIELD, attributes);
        return d;
    }

    private Content hitTerm(String term, ColumnVisibility visibility, long timestamp) {
        Content hitTerm = new Content(term, new Key("row", "dt\0uid", "", visibility, timestamp), true);
        hitTerm.setColumnVisibility(visibility);
        return hitTerm;
    }

    private void apply(Document d) {
        Map.Entry<Key,Document> entry = Maps.immutableEntry(docKey, d);
        new RemoveHitTermGroupingContextTransform().apply(entry);
    }

    private Set<String> hitTerms(Document d) {
        Set<String> terms = new TreeSet<>();
        Attribute<?> hitTerms = d.get(HIT_TERM_FIELD);
        if (hitTerms instanceof Attributes) {
            for (Attribute<?> attribute : ((Attributes) hitTerms).getAttributes()) {
                terms.add(((Content) attribute).getContent());
            }
        } else if (hitTerms != null) {
            terms.add(((Content) hitTerms).getContent());
        }
        return terms;
    }
}
