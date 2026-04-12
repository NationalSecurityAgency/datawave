package datawave.query.function;

import static datawave.query.function.DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE;
import static datawave.query.function.DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Maps;

import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.DocumentFunctions;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.jexl.visitors.DocumentMatchFunctionVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.postprocessing.tf.TermOffsetMap;
import datawave.query.util.Tuple3;

public class JexlEvaluationTest {

    public static final DocumentMatchContext.Limits TEST_DOCUMENT_MATCH_LIMITS = new DocumentMatchContext.Limits(1024, DEFAULT_MAX_DECODED_SIZE,
                    DEFAULT_MAX_ENCODED_CONTEXT_SIZE);

    @Test
    public void testSimpleQuery() {
        String query = "FOO == 'bar'";
        Document d = new Document();
        d.put("FOO", new Content("bar", new Key("shard", "datatype\0uid"), true));

        assertEvaluation(query, new Key("shard", "datatype\0uid"), d, contextFactory(d, Collections.singleton("FOO")));
    }

    @Test
    public void testRegexIntersection() {
        String query = "FOO == 'bar' && FOO =~ 'baz.*'";
        Document d = new Document();
        d.put("FOO", new Content("bar", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));

        assertEvaluation(query, new Key("shard", "datatype\0uid"), d, contextFactory(d, Collections.singleton("FOO")));
    }

    @Test
    public void testRegexCaseIntersection() {
        Document d = new Document();
        d.put("FOO", new Content("Bar", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Numeric("123", new Key("shard", "datatype\0uid"), true));

        Supplier<DatawaveJexlContext> contextSupplier = contextFactory(d, Collections.singleton("FOO"));

        // match the original value
        String query = "FOO == 'bar' && FOO =~ '12.*'";
        assertEvaluation(query, new Key("shard", "datatype\0uid"), d, contextSupplier);

        // match the normalized value
        query = "FOO == 'bar' && FOO =~ '\\+cE1\\.2.*'";
        assertEvaluation(query, new Key("shard", "datatype\0uid"), d, contextSupplier);
    }

    @Test
    public void testRegexUnion() {
        String query = "FOO == 'bar' || FOO =~ 'baz.*'";
        Document d = new Document();
        d.getMetadata();
        d.put("FOO", new Content("bar", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));

        assertEvaluation(query, new Key("shard", "datatype\0uid"), d, contextFactory(d, Collections.singleton("FOO")));
    }

    @Test
    public void testHitTermSource() {
        String query = "FOO == 'bar'";
        Document d = new Document();
        Key expectedMetadata;
        Content hitTermSource = new Content("bar", expectedMetadata = new Key("shard", "datatype\0uid1"), true);
        d.put("FOO", hitTermSource);
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid2"), true));

        assertEvaluation(query, new Key("shard", "datatype\0uid"), d, contextFactory(d, Collections.singleton("FOO")));

        Attributes hitTerm = (Attributes) d.getDictionary().get("HIT_TERM");
        assertEquals(1, hitTerm.getAttributes().size());
        Attribute<?> attribute = hitTerm.getAttributes().iterator().next();
        assertEquals(expectedMetadata, attribute.getMetadata());
        assertEquals(Content.class, attribute.getClass());
        assertEquals(hitTermSource, ((Content) attribute).getSource());
    }

    @Test
    public void testSomeFilterFunctions() {
        String query = "ANCHOR == 'a' && filter:includeRegex(FOO, 'baz.*')";
        Document d = new Document();
        d.put("ANCHOR", new Content("a", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));

        // Case 1: Single fielded filter function, field is present
        evaluate(query, d);

        query = "ANCHOR == 'a' && filter:includeRegex((FOO||FOO2||FOO3), 'baz.*')";
        String orderMattersQuery = "ANCHOR == 'a' && filter:includeRegex((FOO3||FOO2||FOO), 'baz.*')";
        d = new Document();
        d.put("ANCHOR", new Content("a", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));
        d.put("FOO3", new Content("bazaar", new Key("shard", "datatype\0uid"), true));

        // Case 2: Multi-fielded filter function, both fields present
        evaluate(query, d);
        evaluate(orderMattersQuery, d);

        d = new Document();
        d.put("ANCHOR", new Content("a", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));
        d.put("FOO3", new Content("nohit", new Key("shard", "datatype\0uid"), true));

        // Case 3: Multi-fielded filter function, only first field is present
        evaluate(query, d);
        evaluate(orderMattersQuery, d);

        d = new Document();
        d.put("ANCHOR", new Content("a", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("nohit", new Key("shard", "datatype\0uid"), true));
        d.put("FOO3", new Content("bazaar", new Key("shard", "datatype\0uid"), true));

        // Case 4: Multi-fielded filter function, only second field is present
        evaluate(query, d);
        evaluate(orderMattersQuery, d);
    }

    // Assume fields are {ANCHOR, FOO, FOO2} and a constant doc key
    private void evaluate(String query, Document d) {
        assertEvaluation(query, new Key("shard", "datatype\0uid"), d, contextFactory(d, Arrays.asList("ANCHOR", "FOO", "FOO2", "FOO3")));
    }

    @Test
    public void testContentPhraseFunction() {
        String query = "FOO == 'bar' && TOKFIELD == 'big' && TOKFIELD == 'red' && TOKFIELD == 'dog' && content:phrase(termOffsetMap, 'big', 'red', 'dog')";

        Map<String,TermFrequencyList> map = new HashMap<>();
        map.put("big", buildTfList("TOKFIELD", 1));
        map.put("red", buildTfList("TOKFIELD", 2));
        map.put("dog", buildTfList("TOKFIELD", 3));

        Key docKey = new Key("shard", "datatype\0uid");

        Document d = new Document();
        d.put("FOO", new Content("bar", docKey, true));
        d.put("TOKFIELD", new Content("big", docKey, true));
        d.put("TOKFIELD", new Content("red", docKey, true));
        d.put("TOKFIELD", new Content("dog", docKey, true));
        assertEvaluation(query, docKey, d, contextFactory(d, Arrays.asList("FOO", "TOKFIELD"),
                        ctx -> ctx.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, new TermOffsetMap(map))));

        // assert that "big red dog" came back in the hit terms
        boolean foundPhrase = false;
        Attributes attrs = (Attributes) d.get("HIT_TERM");
        for (Attribute<?> attr : attrs.getAttributes()) {
            if (attr.getData().equals("TOKFIELD:big red dog")) {
                foundPhrase = true;
            }
        }
        assertEquals(5, attrs.size());
        assertTrue(foundPhrase);
    }

    @Test
    public void testDocumentMatchAddsDocumentAttribute() {
        String query = "FOO == 'bar' && document:match('car')";
        Key docKey = new Key("shard", "datatype\0uid");
        Document d = new Document();
        d.put("FOO", new Content("bar", docKey, true));

        final List<Map.Entry<Key,Value>> entries = List
                        .of(Maps.immutableEntry(new Key("row", "d", "datatype\0uid\0BODY", "A"), new Value(buildEncodedValue("scar car"))));
        assertEvaluation(query, docKey, d, contextFactory(d, Collections.singleton("FOO"), ctx -> ctx
                        .set(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME, new DocumentMatchContext(entries, TEST_DOCUMENT_MATCH_LIMITS))));
        assertEquals(Collections.singleton("{\"view\":\"BODY\",\"matches\":{\"car\":[1,5]}}"),
                        getDocumentMatchContents(d.get(DocumentFunctions.DOCUMENT_MATCHES)));
        assertEquals(new ColumnVisibility("A"), d.get(DocumentFunctions.DOCUMENT_MATCHES).getColumnVisibility());
    }

    @Test
    public void testDocumentMatchAddsPerEntryDocumentAttributesAcrossCalls() {
        String query = "FOO == 'bar' && document:match('BODY', 'car') && document:match('CONTENT2', 'lawyer')";
        Key docKey = new Key("shard", "datatype\0uid");
        Document d = new Document();
        d.put("FOO", new Content("bar", docKey, true));

        final List<Map.Entry<Key,Value>> entries = List.of(
                        Maps.immutableEntry(new Key("row", "d", "datatype\0uid\0BODY", "A"), new Value(buildEncodedValue("scar car"))),
                        Maps.immutableEntry(new Key("row", "d", "datatype\0uid\0CONTENT2", "A"), new Value(buildEncodedValue("lawyer car"))));
        assertEvaluation(query, docKey, d, contextFactory(d, Collections.singleton("FOO"), ctx -> ctx
                        .set(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME, new DocumentMatchContext(entries, TEST_DOCUMENT_MATCH_LIMITS))));
        assertEquals(Set.of("{\"view\":\"BODY\",\"matches\":{\"car\":[1,5]}}", "{\"view\":\"CONTENT2\",\"matches\":{\"lawyer\":[0]}}"),
                        getDocumentMatchContents(d.get(DocumentFunctions.DOCUMENT_MATCHES)));
    }

    @Test
    public void testDocumentMatchAccumulatesCallsWithinSameEntry() {
        String query = "FOO == 'bar' && document:match('BODY', 'car') && document:match('BODY', 'lawyer')";
        Key docKey = new Key("shard", "datatype\0uid");
        Document d = new Document();
        d.put("FOO", new Content("bar", docKey, true));

        final List<Map.Entry<Key,Value>> entries = List
                        .of(Maps.immutableEntry(new Key("row", "d", "datatype\0uid\0BODY", "A"), new Value(buildEncodedValue("scar car lawyer"))));
        assertEvaluation(query, docKey, d, contextFactory(d, Collections.singleton("FOO"), ctx -> ctx
                        .set(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME, new DocumentMatchContext(entries, TEST_DOCUMENT_MATCH_LIMITS))));
        assertEquals(Collections.singleton("{\"view\":\"BODY\",\"matches\":{\"car\":[1,5],\"lawyer\":[9]}}"),
                        getDocumentMatchContents(d.get(DocumentFunctions.DOCUMENT_MATCHES)));
    }

    @Test
    public void testDocumentMatchPreservesPerEntryVisibilities() {
        String query = "FOO == 'bar' && document:match('BODY', 'car') && document:match('CONTENT2', 'lawyer')";
        Key docKey = new Key("shard", "datatype\0uid");
        Document d = new Document();
        d.put("FOO", new Content("bar", docKey, true));

        final List<Map.Entry<Key,Value>> entries = List.of(
                        Maps.immutableEntry(new Key("row", "d", "datatype\0uid\0BODY", "A"), new Value(buildEncodedValue("scar car"))),
                        Maps.immutableEntry(new Key("row", "d", "datatype\0uid\0CONTENT2", "B"), new Value(buildEncodedValue("lawyer car"))));
        assertEvaluation(query, docKey, d, contextFactory(d, Collections.singleton("FOO"), ctx -> ctx
                        .set(DocumentFunctions.DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME, new DocumentMatchContext(entries, TEST_DOCUMENT_MATCH_LIMITS))));

        Attributes matches = assertInstanceOf(Attributes.class, d.get(DocumentFunctions.DOCUMENT_MATCHES));
        Map<String,ColumnVisibility> visibilitiesByPayload = new HashMap<>();
        for (Attribute<? extends Comparable<?>> attribute : matches.getAttributes()) {
            Content content = assertInstanceOf(Content.class, attribute);
            visibilitiesByPayload.put(content.getContent(), content.getColumnVisibility());
        }

        assertEquals(new ColumnVisibility("A"), visibilitiesByPayload.get("{\"view\":\"BODY\",\"matches\":{\"car\":[1,5]}}"));
        assertEquals(new ColumnVisibility("B"), visibilitiesByPayload.get("{\"view\":\"CONTENT2\",\"matches\":{\"lawyer\":[0]}}"));
    }

    private Set<String> getDocumentMatchContents(Attribute<?> attribute) {
        Set<String> values = new LinkedHashSet<>();
        if (attribute instanceof Attributes) {
            for (Attribute<? extends Comparable<?>> child : ((Attributes) attribute).getAttributes()) {
                values.add(((Content) child).getContent());
            }
        } else {
            values.add(((Content) attribute).getContent());
        }
        return values;
    }

    private byte[] buildEncodedValue(String content) {
        try {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.OutputStream b64s = java.util.Base64.getEncoder().wrap(bos);
            java.util.zip.GZIPOutputStream gzip = new java.util.zip.GZIPOutputStream(b64s);
            gzip.write(content.getBytes());
            gzip.close();
            b64s.close();
            bos.close();
            return bos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCompareFunction() {
        // eq op
        String query = "FOO == 'bar' && filter:compare(FIELD_A,'==','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_A,'==','any',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'==','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'==','any',FIELD_B)";
        testCompare(query, true);

        // eq op, alternate form
        query = "FOO == 'bar' && filter:compare(FIELD_A,'=','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_A,'=','any',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'=','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'=','any',FIELD_B)";
        testCompare(query, true);

        // lt op
        query = "FOO == 'bar' && filter:compare(FIELD_A,'<','all',FIELD_B)";
        testCompare(query, true);

        query = "FOO == 'bar' && filter:compare(FIELD_A,'<','any',FIELD_B)";
        testCompare(query, true);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'<','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'<','any',FIELD_B)";
        testCompare(query, true);

        // le op
        query = "FOO == 'bar' && filter:compare(FIELD_A,'<=','all',FIELD_B)";
        testCompare(query, true);

        query = "FOO == 'bar' && filter:compare(FIELD_A,'<=','any',FIELD_B)";
        testCompare(query, true);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'<=','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'<=','any',FIELD_B)";
        testCompare(query, true);

        // gt op
        query = "FOO == 'bar' && filter:compare(FIELD_A,'>','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_A,'>','any',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'>','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'>','any',FIELD_B)";
        testCompare(query, true);

        // ge op
        query = "FOO == 'bar' && filter:compare(FIELD_A,'>=','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_A,'>=','any',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'>=','all',FIELD_B)";
        testCompare(query, false);

        query = "FOO == 'bar' && filter:compare(FIELD_C,'>=','any',FIELD_B)";
        testCompare(query, true);
    }

    private void testCompare(String query, boolean expected) {

        // populate doc
        Key docKey = new Key("shard", "datatype\0uid");
        Document d = new Document();
        d.put("FOO", new Content("bar", docKey, true));
        d.put("FIELD_A", new Content("apple", docKey, true));
        d.put("FIELD_A", new Content("banana", docKey, true));
        d.put("FIELD_B", new Content("xylophone", docKey, true));
        d.put("FIELD_B", new Content("zephyr", docKey, true));
        d.put("FIELD_C", new Content("zebra", docKey, true));
        d.put("FIELD_C", new Content("zephyr", docKey, true));

        assertEvaluation(query, docKey, d, contextFactory(d, Arrays.asList("FOO", "FIELD_A", "FIELD_B", "FIELD_C")), expected);
    }

    private void assertEvaluation(String query, Key key, Document d, Supplier<DatawaveJexlContext> contextSupplier) {
        assertEvaluation(query, key, d, contextSupplier, true);
    }

    private void assertEvaluation(String query, Key key, Document d, Supplier<DatawaveJexlContext> contextSupplier, boolean expected) {
        JexlEvaluation evaluation = new JexlEvaluation(rewriteDocumentMatchFunctions(query));
        boolean result = evaluation.apply(new Tuple3<>(key, d, contextSupplier.get()));
        assertEquals(expected, result);

        evaluation = new JexlEvaluation(rewriteDocumentMatchFunctions(query), new HitListArithmetic());
        result = evaluation.apply(new Tuple3<>(key, d, contextSupplier.get()));
        assertEquals(expected, result);
    }

    private Supplier<DatawaveJexlContext> contextFactory(Document document, Collection<String> fields) {
        return contextFactory(document, fields, context -> {});
    }

    private Supplier<DatawaveJexlContext> contextFactory(Document document, Collection<String> fields, Consumer<DatawaveJexlContext> customizer) {
        return () -> {
            DatawaveJexlContext context = new DatawaveJexlContext();
            document.visit(fields, context);
            customizer.accept(context);
            return context;
        };
    }

    private String rewriteDocumentMatchFunctions(String query) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            if (!DocumentMatchFunctionVisitor.rewrite(script)) {
                return query;
            }
            return JexlStringBuildingVisitor.buildQueryWithoutParse(script);
        } catch (org.apache.commons.jexl3.parser.ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private TermFrequencyList buildTfList(String field, int... offsets) {
        TermFrequencyList.Zone zone = buildZone(field);
        List<TermWeightPosition> position = buildTermWeightPositions(offsets);
        return new TermFrequencyList(Maps.immutableEntry(zone, position));
    }

    private TermFrequencyList.Zone buildZone(String field) {
        return new TermFrequencyList.Zone(field, true, "shard\0datatype\0uid");
    }

    private List<TermWeightPosition> buildTermWeightPositions(int... offsets) {
        List<TermWeightPosition> list = new ArrayList<>();
        for (int offset : offsets) {
            list.add(new TermWeightPosition.Builder().setOffset(offset).setZeroOffsetMatch(true).build());
        }
        return list;
    }
}
