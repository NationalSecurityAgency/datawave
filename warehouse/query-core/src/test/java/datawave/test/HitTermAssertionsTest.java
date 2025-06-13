package datawave.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.util.Tuple3;
import datawave.query.util.TypeMetadata;

public class HitTermAssertionsTest {

    private final HitTermAssertions hitTermAssertions = new HitTermAssertions();
    private final Key documentKey = new Key("row", "datatype\0uid");
    private final Set<String> fieldNames = Set.of("F");

    private final AttributeFactory attributeFactory = new AttributeFactory(new TypeMetadata());

    private String query;

    @BeforeEach
    public void setup() {
        hitTermAssertions.resetState();
    }

    @Test
    public void testSingleHitTerm() {
        withQuery("F == '1'");
        hitTermAssertions.withRequiredAllOf("F:1");

        Document document = createDocument("F", "1");
        drive(document);
    }

    @Test
    public void testIntersection() {
        withQuery("F == '1' && F == '2'");
        hitTermAssertions.withRequiredAllOf("F:1", "F:2");
        Document document = createDocument("F", "1", "2");
        drive(document);
    }

    @Test
    public void testUnion() {
        withQuery("F == '1' || F == '2'");
        hitTermAssertions.withRequiredAnyOf("F:1", "F:2");
        Document document = createDocument("F", "1", "2");
        drive(document);

        document = createDocument("F", "1");
        drive(document);

        document = createDocument("F", "2");
        drive(document);
    }

    @Test
    public void testIntersectionWithNestedUnion() {
        withQuery("F == '1' && (F == '2' || F == '3')");
        hitTermAssertions.withRequiredAllOf("F:1");
        hitTermAssertions.withRequiredAnyOf("F:2", "F:3");

        Document document = createDocument("F", "1", "2", "3");
        drive(document);

        document = createDocument("F", "1", "2");
        drive(document);

        document = createDocument("F", "1", "3");
        drive(document);
    }

    @Test
    public void testUnionWithNestedIntersection() {
        withQuery("F == '1' || (F == '2' && F == '3')");
        hitTermAssertions.withOptionalAllOf("F:1");
        hitTermAssertions.withOptionalAllOf("F:2", "F:3");

        Document document = createDocument("F", "1", "2", "3");
        drive(document);

        document = createDocument("F", "1");
        drive(document);

        document = createDocument("F", "2", "3");
        drive(document);
    }

    @Test
    public void testDoubleNestedIntersection() {
        withQuery("(F == '1' && F == '2') || (F == '3' && F == '4')");
        hitTermAssertions.withOptionalAllOf("F:1", "F:2");
        hitTermAssertions.withOptionalAllOf("F:3", "F:4");

        Document document = createDocument("F", "1", "2", "3", "4");
        drive(document);

        document = createDocument("F", "1", "2");
        drive(document);

        document = createDocument("F", "3", "4");
        drive(document);
    }

    @Test
    public void testDoubleNestedUnion() {
        withQuery("(F == '1' || F == '2') && (F == '3' || F == '4')");
        hitTermAssertions.withRequiredAnyOf("F:1", "F:2");
        hitTermAssertions.withRequiredAnyOf("F:3", "F:4");

        Document document = createDocument("F", "1", "2", "3", "4");
        drive(document);

        document = createDocument("F", "1", "3");
        drive(document);

        document = createDocument("F", "2", "4");
        drive(document);
    }

    @Test
    public void testOnlyOptionalHits() {
        withQuery("F == '1' || F == '2'");
        hitTermAssertions.withOptionalAnyOf("F:1", "F:2");

        Document document = createDocument("F", "1", "2", "3", "4");
        drive(document);

        document = createDocument("F", "1");
        drive(document);

        document = createDocument("F", "2");
        drive(document);

        document = createDocument("F", "5");
        drive(document, false);
    }

    @Test
    public void testNotEnoughMinerals() {
        // handles the case where more hit terms were returned than requested
        withQuery("F == '1' && F == '2'");
        hitTermAssertions.withRequiredAllOf("F:1");

        // should complain about extra hit term {F:2}
        Document document = createDocument("F", "1", "2");
        assertThrows(IllegalArgumentException.class, () -> drive(document));
    }

    private void drive(Document document) {
        drive(document, true);
    }

    private void drive(Document document, boolean expected) {
        JexlEvaluation evaluation = new JexlEvaluation(query, new HitListArithmetic(true));

        DatawaveJexlContext context = new DatawaveJexlContext();
        document.visit(fieldNames, context);

        boolean result = evaluation.apply(new Tuple3<>(null, document, context));

        boolean validated = hitTermAssertions.assertHitTerms(document);
        assertEquals(expected, validated);
    }

    private void withQuery(String query) {
        this.query = query;
    }

    private Document createDocument(String field, String... values) {
        Document d = new Document();
        for (String value : values) {
            d.put(field, attributeFactory.create(field, value, documentKey, true));
        }
        return d;
    }
}
