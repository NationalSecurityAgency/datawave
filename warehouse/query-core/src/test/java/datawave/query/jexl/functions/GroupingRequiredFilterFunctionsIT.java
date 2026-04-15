package datawave.query.jexl.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
    private final Set<String> fields = Set.of("FIELD", "FIELD1");

    private String query;
    private Document document;
    private AttributeFactory attributeFactory;

    @BeforeEach
    public void setup() {
        this.query = null;
        this.document = new Document();

        TypeMetadata metadata = new TypeMetadata();
        metadata.put("FIELD", "datatype", LcType.class.getTypeName());
        metadata.put("FIELD1", "datatype", LcType.class.getTypeName());

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

    // fields are ; delimited
    // query;data1,data2,data3,...dataN;result
    @CsvSource(delimiter = ';', value = {
            // matchesInGroup() by default only matches on the last value AFTER the LAST .
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.3=a,FIELD1.1.2.3=b; true",
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.3=a,FIELD1.1.2.3=b; true",
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.3=a,FIELD1.1.1.3=b; true",
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.3=a,FIELD1.2.2.3=b; true",
            // none of these should match even though they match on the left
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.0=a,FIELD1.1.2.1=b; false",
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.0=a,FIELD1.1.1.1=b; false",
            // no overlap should never match
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.0=a,FIELD1.0.1.1=b; false",
            // it does not matter if there are the same number of groups, as long as the last one matches
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD.1.2=a,FIELD1.2.2.2=b; true",
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD.2=a,FIELD1.2.2.2=b; true",
            // there must be at least one group on each field
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD=a,FIELD1.2.2.2=b; false",
            // even if neither has a group it doesn't match in group
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b'); FIELD=a,FIELD1=b; false",
            // can match on different values on the same field as long as they are in the same group
            "grouping:matchesInGroup(FIELD, 'a', FIELD, 'b'); FIELD.1.2.3=a,FIELD.1.2.3=b; true",

            // matches in groups final argument dictates the index of the grouping notation from the RIGHT (0 indexed)
            // 0 - is the same as default, match rightmost group only
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b', 0); FIELD.0.0.3=a,FIELD1.9.9.3=b; true",
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b', 0); FIELD.0.0.0=a,FIELD1.9.9.9=b; false",
            // 1 - match two rightmost groups only
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b', 1); FIELD.1.2.3=a,FIELD1.0.2.3=b; true",
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b', 1); FIELD.1.2.3=a,FIELD1.1.1.3=b; false",
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b', 1); FIELD.1.2.3=a,FIELD1.1.2.4=b; false",
            // 2 - can match all grouping
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b', 2); FIELD.1.2.3=a,FIELD1.1.2.3=b; true",
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b', 2); FIELD.1.2.3=a,FIELD1.0.2.3=b; false",
            // 10 - if there aren't enough indexes in the group it can't be true even if otherwise is a match
            "grouping:matchesInGroup(FIELD, 'a', FIELD1, 'b', 10); FIELD.1.2.3=a,FIELD1.1.2.3=b; false",

            // supports regexes on either argument
            "grouping:matchesInGroup(FIELD, 'a*', FIELD1, 'b*'); FIELD.1.2.3=aaaaaaaa,FIELD1.1.2.3=bbbbbbbbbb; true",
            // regex can be full wildcards
            "grouping:matchesInGroup(FIELD, '.*', FIELD1, 'b*'); FIELD.1.2.3=aaaaaaaa,FIELD1.1.2.3=bbbbbbbbbb; true",
            // regex can include range quantifiers
            "grouping:matchesInGroup(FIELD, 'a{8}', FIELD1, 'b*'); FIELD.1.2.3=aaaaaaaa,FIELD1.1.2.3=bbbbbbbbbb; true",
            // can be complex patterns with lookahead: one digit, one upper case, 8+ characters
            "grouping:matchesInGroup(FIELD, '^(?=.*\\\\d)(?=.*[A-Z]).{8,}$', FIELD1, 'b*'); FIELD.1.2.3=bb7dfZuq,FIELD1.1.2.3=bbbbbbbbbb; true",

            // matchesInGroupLeft should work the same as matchesInGroup but with an index offset from the left but
            // currently does not. The current behavior is show below in tests. When unexpected, the correct response
            // is indicated. Problem appears to be in EvaluationPhaseFilterFunctions.getMatchToLeftOfPeriod()
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.3=a,FIELD1.1.2.3=b; true",
            // CURRENT BEHAVIOR: 0-index is acting like 1-index, this should still be true
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b'); FIELD.1.0.3=a,FIELD1.1.9.3=b; false",
            // CURRENT BEHAVIOR: 1-index is acting like 0-index, this should still be false
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b', 1); FIELD.000.999.0=a,FIELD1.000.888.9=b; true",
            // CURRENT BEHAVIOR: 2-index is acting like 3-index, this should be true
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b', 2); FIELD.000.999.1=a,FIELD1.000.999.1=b; false",

            // 0 - 1 does not match 0
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.3=a,FIELD1.0.2.3=b; false",
            // 0 - explicit
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b', 0); FIELD.1.2.3=a,FIELD1.0.2.3=b; false",
            // 0 - 1 does not match 0
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.3=a,FIELD1.0.3.3=b; false",
            // 0 - explicit
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b', 0); FIELD.1.2.3=a,FIELD1.0.3.3=b; false",
            // 0 - 1 does not match 0
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b'); FIELD.1.2.3=a,FIELD1.0.2.4=b; false",
            // 0 - explicit
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b', 0); FIELD.1.2.3=a,FIELD1.0.2.4=b; false",
            // 1 - 000.9999 matches 000.999
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b', 1); FIELD.000.999.0=a,FIELD1.000.999.9=b; true",
            // 1 - 000.999 does not match 111.999
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b', 1); FIELD.000.999.0=a,FIELD1.111.999.9=b; false",
            // 3 - if there aren't enough indexes in the group it can't be true even if otherwise is a match
            "grouping:matchesInGroupLeft(FIELD, 'a', FIELD1, 'b', 3); FIELD.000.999.0=a,FIELD1.000.999.0=b; false",

            // matchesInGroupLeft also supports regex
            "grouping:matchesInGroupLeft(FIELD, 'a*', FIELD1, 'b*'); FIELD.1.2.3=aaaaaaaa,FIELD1.1.2.3=bbbbbbbbbb; true",
            "grouping:matchesInGroupLeft(FIELD, 'a{8}', FIELD1, 'b*'); FIELD.1.2.3=aaaaaaaa,FIELD1.1.2.3=bbbbbbbbbb; true",
            "grouping:matchesInGroupLeft(FIELD, '^(?=.*\\\\d)(?=.*[A-Z]).{8,}$', FIELD1, 'b*'); FIELD.1.2.3=bb7dfZuq,FIELD1.1.2.3=bbbbbbbbbb; true",})
    @ParameterizedTest(name = "{0} against {1} should be {2}")
    public void functionalTests(String query, String data, boolean result) {
        withQuery(query);
        for (String kv : data.split(",")) {
            String[] kvSplit = kv.split("=");
            withData(kvSplit[0], kvSplit[1]);
        }
        evaluate(result);
    }

    @Test
    public void testTLD_matchesInGroup_miss() {
        withQuery("grouping:matchesInGroup(FIELD, 'a', FIELD, 'b')");
        withData("FIELD.1.2.3", "a", "uid.1");
        withData("FIELD.1.2.3", "b", "uid.2");
        // grouping should not match across different child documents
        evaluate(false);
    }

    @Test
    public void testTLD_matchesInGroup_hit() {
        withQuery("grouping:matchesInGroup(FIELD, 'a', FIELD, 'b')");
        withData("FIELD.1.2.3", "a", "uid.1.2");
        withData("FIELD.1.2.3", "b", "uid.1.2");
        // grouping will match in the same child document
        evaluate(true);
    }

    @Test
    public void testTLD_matchesInGroupLeft_miss() {
        withQuery("grouping:matchesInGroupLeft(FIELD, 'a', FIELD, 'b')");
        withData("FIELD.1.2.3", "a", "uid.1");
        withData("FIELD.1.2.3", "b", "uid.2");
        // grouping should not match across different child documents
        evaluate(false);
    }

    @Test
    public void testTLD_matchesInGroupLeft_hit() {
        withQuery("grouping:matchesInGroupLeft(FIELD, 'a', FIELD, 'b')");
        withData("FIELD.1.2.3", "a", "uid.1.2");
        withData("FIELD.1.2.3", "b", "uid.1.2");
        // grouping will match in the same child document
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
