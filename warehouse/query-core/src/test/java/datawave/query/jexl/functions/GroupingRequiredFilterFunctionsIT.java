package datawave.query.jexl.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.type.LcType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.util.Tuple3;
import datawave.query.util.TypeMetadata;

/**
 * Integration style test that evaluates grouping functions against various data sets
 */
public class GroupingRequiredFilterFunctionsIT {

    private final Key docKey = new Key("20250703", "datatype\0uid");
    private final Set<String> fields = Set.of("FIELD");

    private String query;
    private Document document;
    private AttributeFactory attributeFactory;

    @BeforeEach
    public void setup() {
        this.query = null;
        this.document = new Document();

        TypeMetadata metadata = new TypeMetadata();
        metadata.put("FIELD", "datatype", LcType.class.getTypeName());

        attributeFactory = new AttributeFactory(metadata);
    }

    public void withQuery(String query) {
        this.query = query;
    }

    public void withData(String field, String value) {
        withData(field, value, "uid");
    }

    public void withData(String field, String value, String uid) {
        Attribute<?> source = attributeFactory.create(field, value, createKey(uid), true);
        document.put(field, source, true);
    }

    @Test
    public void testMatchesInGroup() {
        withQuery("grouping:matchesInGroup(FIELD, 'a', FIELD, 'b')");
        withData("FIELD.1.2.3", "a");
        withData("FIELD.1.2.3", "b");
        evaluate(true);
    }

    @Test
    public void testMatchesInGroupWithIndex() {
        withQuery("grouping:matchesInGroup(FIELD, 'a', FIELD, 'b', 1)");
        withData("FIELD.1.2.3", "a");
        withData("FIELD.1.2.3", "b");
        evaluate(true);
    }

    @Test
    public void testMatchesInGroupWithIndex_noMatch() {
        withQuery("grouping:matchesInGroup(FIELD, 'a', FIELD, 'b', 1)");
        withData("FIELD.1.2.3", "a");
        withData("FIELD.1.1.3", "b");
        evaluate(false);
    }

    @Test
    public void testMatchesInGroupWithIndex_indexHigherThanGroupsAvailable() {
        withQuery("grouping:matchesInGroup(FIELD, 'a', FIELD, 'b', 7)");
        withData("FIELD.1.2.3", "a");
        withData("FIELD.1.2.3", "b");
        evaluate(false);
    }

    @Test
    public void testMatchesInGroupLeft() {
        withQuery("grouping:matchesInGroupLeft(FIELD, 'a', FIELD, 'b')");
        withData("FIELD.1.2.3", "a");
        withData("FIELD.1.2.3", "b");
        evaluate(true);
    }

    @Test
    public void testMatchesInGroupLeft_withIndex() {
        withQuery("grouping:matchesInGroupLeft(FIELD, 'a', FIELD, 'b', 1)");
        withData("FIELD.1.2.4", "a");
        withData("FIELD.1.2.7", "b");
        evaluate(true);
    }

    @Test
    public void testTLD_matchesInGroup() {
        withQuery("grouping:matchesInGroup(FIELD, 'a', FIELD, 'b')");
        withData("FIELD.1.2.3", "a", "uid.1");
        withData("FIELD.1.2.3", "b", "uid.2");
        // grouping should not match across different child documents
        evaluate(true);
    }

    private void evaluate(boolean expected) {
        DatawaveJexlContext context = new DatawaveJexlContext();
        document.visit(fields, context);

        JexlEvaluation evaluation = new JexlEvaluation(query);
        boolean result = evaluation.apply(new Tuple3<>(docKey, document, context));
        assertEquals(expected, result);
    }

    private Key createKey(String uid) {
        return new Key("20250703", "datatype\0" + uid);
    }
}
