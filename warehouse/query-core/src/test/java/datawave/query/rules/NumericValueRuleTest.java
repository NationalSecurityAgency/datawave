package datawave.query.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.util.TypeMetadata;

class NumericValueRuleTest extends ShardQueryRuleTest {

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    @Test
    void testQueryWithoutNumericValues() throws Exception {
        givenQuery("FOO == 'abc' && BAR != 'abc' || HAT > 'abc' || BAT < 'abc' || HEN <= 'abc' || VEE >= 'abc'");

        // Expect an exception due to missing TypeMetadata
        expectException(new IllegalStateException("TypeMetadata should not be null."));
        assertResult();
    }

    @Test
    void testQueryWithNumericValuesForNumericFields() throws Exception {
        givenQuery("FOO == 1 && BAR != '1' || HAT > 1 || BAT < 1 || HEN <= 1 || VEE >= 1");

        // Populate TypeMetadata with field type information.
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FOO", "test", NumberType.class.getName());
        typeMetadata.put("BAR", "test", NumberType.class.getName());
        typeMetadata.put("HAT", "test", NumberType.class.getName());
        typeMetadata.put("BAT", "test", NumberType.class.getName());
        typeMetadata.put("HEN", "test", NumberType.class.getName());
        typeMetadata.put("VEE", "test", NumberType.class.getName());
        givenTypeMetadata(typeMetadata);

        // Do not expect any messages.
        assertResult();
    }

    @Test
    void testNumericValuesIgnoredForNonNumericFields() throws Exception {
        // Numeric literals without range operators should not trigger messages.
        givenQuery("FOO == 1 && BAR == 2");

        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FOO", "test", LcNoDiacriticsType.class.getName());
        typeMetadata.put("BAR", "test", LcNoDiacriticsType.class.getName());
        givenTypeMetadata(typeMetadata);

        // No messages expected because there are no range operators.
        assertResult();
    }

    @Test
    void testRangesOnMixedTypedFields() throws Exception {
        // FOO is numeric and has a proper range; others are ranges on non-numeric fields.
        givenQuery("(FOO >= 1 && FOOBAR <= 2) || BAR > 5 || BAT < 10 || HEN <= 15 || VEE >= 20");

        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FOO", "test", NumberType.class.getName());
        typeMetadata.put("FOOBAR", "test", NumberType.class.getName());
        typeMetadata.put("BAR", "test", LcNoDiacriticsType.class.getName());
        typeMetadata.put("BAT", "test", LcNoDiacriticsType.class.getName());
        typeMetadata.put("HEN", "test", LcNoDiacriticsType.class.getName());
        typeMetadata.put("VEE", "test", LcNoDiacriticsType.class.getName());
        givenTypeMetadata(typeMetadata);

        expectMessage("Range values supplied for non-numeric field(s): BAR, BAT, HEN, VEE");

        assertResult();
    }

    @Test
    void testLuceneRangesConvertedAndValidated() throws Exception {
        String lucene = "A:[1 TO 2] OR B:[* TO 10} AND C:'xyz' OR D:{-5 TO -1}";
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        QueryNode node = parser.parse(lucene);
        String jexl = node.toString();

        givenQuery(jexl);

        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("A", "test", NumberType.class.getName());
        typeMetadata.put("B", "test", LcNoDiacriticsType.class.getName());
        typeMetadata.put("D", "test", LcNoDiacriticsType.class.getName());
        givenTypeMetadata(typeMetadata);

        expectMessage("Range values supplied for non-numeric field(s): B, D");

        assertResult();
    }

    @Test
    void testEdgeCasesNegativeAndUnbounded() throws Exception {
        // Mixed open/closed range expressions in JEXL.
        givenQuery("X > -1.5 && X <= 10.25 && Y >= 0 || Z < 0");

        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("X", "test", NumberType.class.getName());
        typeMetadata.put("Y", "test", LcNoDiacriticsType.class.getName());
        typeMetadata.put("Z", "test", LcNoDiacriticsType.class.getName());
        givenTypeMetadata(typeMetadata);

        expectMessage("Range values supplied for non-numeric field(s): Y, Z");

        assertResult();
    }

    @Test
    void testNegatedRangeStillValidated() throws Exception {
        // Even within negation, range usage should still be validated.
        givenQuery("!(Q >= 1 && Q < 10)");

        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("Q", "test", LcNoDiacriticsType.class.getName());
        givenTypeMetadata(typeMetadata);

        expectMessage("Range values supplied for non-numeric field(s): Q");

        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToJexl();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new NumericValueRule(ruleName);
    }
}
