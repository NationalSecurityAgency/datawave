package datawave.query.rules;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;
import datawave.query.util.TypeMetadata;

class NumericValueRuleTest extends ShardQueryRuleTest {

    private static final Set<String> NUMBER_TYPE = Set.of(NumberType.class.getName());
    private static final Set<String> LC_NO_DIACRITICS_TYPE = Set.of(LcNoDiacriticsType.class.getName());

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

        // Set up a mock TypeMetadata that will return field type information.
        TypeMetadata typeMetadata = mock(TypeMetadata.class);
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("FOO");
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("BAR");
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("HAT");
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("BAT");
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("HEN");
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("VEE");
        givenTypeMetadata(typeMetadata);

        // Do not expect any messages.
        assertResult();
    }

    @Test
    void testNumericValuesIgnoredForNonNumericFields() throws Exception {
        // Numeric literals without range operators should not trigger messages.
        givenQuery("FOO == 1 && BAR == 2");

        TypeMetadata typeMetadata = mock(TypeMetadata.class);
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("FOO");
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("BAR");
        givenTypeMetadata(typeMetadata);

        // No messages expected because there are no range operators.
        assertResult();
    }

    @Test
    void testRangesOnMixedTypedFields() throws Exception {
        // FOO is numeric and has a proper range; others are ranges on non-numeric fields.
        givenQuery("(FOO >= 1 && FOOBAR <= 2) || BAR > 5 || BAT < 10 || HEN <= 15 || VEE >= 20");

        TypeMetadata typeMetadata = mock(TypeMetadata.class);
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("FOO");
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("FOOBAR");
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("BAR");
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("BAT");
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("HEN");
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("VEE");
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

        TypeMetadata typeMetadata = mock(TypeMetadata.class);
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("A");
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("B");
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("D");
        givenTypeMetadata(typeMetadata);

        expectMessage("Range values supplied for non-numeric field(s): B, D");

        assertResult();
    }

    @Test
    void testEdgeCasesNegativeAndUnbounded() throws Exception {
        // Mixed open/closed range expressions in JEXL.
        givenQuery("X > -1.5 && X <= 10.25 && Y >= 0 || Z < 0");

        TypeMetadata typeMetadata = mock(TypeMetadata.class);
        doReturn(NUMBER_TYPE).when(typeMetadata).getNormalizerNamesForField("X");
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("Y");
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("Z");
        givenTypeMetadata(typeMetadata);

        expectMessage("Range values supplied for non-numeric field(s): Y, Z");

        assertResult();
    }

    @Test
    void testNegatedRangeStillValidated() throws Exception {
        // Even within negation, range usage should still be validated.
        givenQuery("!(Q >= 1 && Q < 10)");

        TypeMetadata typeMetadata = mock(TypeMetadata.class);
        doReturn(LC_NO_DIACRITICS_TYPE).when(typeMetadata).getNormalizerNamesForField("Q");
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
