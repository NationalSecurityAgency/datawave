package datawave.query.transformer;

import static datawave.query.function.JexlEvaluation.HIT_TERM_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
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
        Attributes attributes = new Attributes(true);
        for (String term : terms) {
            Content hitTerm = new Content(term, docKey, true);
            hitTerm.setColumnVisibility(cv);
            attributes.add(hitTerm);
        }

        Document d = new Document();
        d.put(HIT_TERM_FIELD, attributes);
        return d;
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
