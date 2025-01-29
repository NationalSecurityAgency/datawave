package datawave.query.rules;

import java.util.Set;

import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
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

        // Do not expect any messages.
        assertResult();
    }

    @Test
    void testQueryWithNumericValuesForNumericFields() throws Exception {
        givenQuery("FOO == 1 && BAR != '1' || HAT > 1 || BAT < 1 || HEN <= 1 || VEE >= 1");

        // Set up a mock TypeMetadata that will return field type information.
        TypeMetadata typeMetadata = EasyMock.mock(TypeMetadata.class);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("FOO")).andReturn(NUMBER_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("BAR")).andReturn(NUMBER_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("HAT")).andReturn(NUMBER_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("BAT")).andReturn(NUMBER_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("HEN")).andReturn(NUMBER_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("VEE")).andReturn(NUMBER_TYPE);
        EasyMock.replay(typeMetadata);
        givenTypeMetadata(typeMetadata);

        // Do not expect any messages.
        assertResult();
    }

    @Test
    void testQueryWithNumericValuesForNonNumericFields() throws Exception {
        givenQuery("FOO == 1 && BAR != '1' || HAT > 1 || BAT < 1 || HEN <= 1 || VEE >= 1");

        // Set up a mock TypeMetadata that will return field type information.
        TypeMetadata typeMetadata = EasyMock.mock(TypeMetadata.class);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("FOO")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("BAR")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("HAT")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("BAT")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("HEN")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("VEE")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.replay(typeMetadata);
        givenTypeMetadata(typeMetadata);

        expectMessage("Numeric values supplied for non-numeric field(s): FOO, BAR, HAT, BAT, HEN, VEE");

        assertResult();
    }

    @Test
    void testQueryWithNumericValuesForMixedTypedFields() throws Exception {
        givenQuery("FOO == 1 && BAR != '1' || HAT > 1 || BAT < 1 || HEN <= 1 || VEE >= 1");

        // Set up a mock TypeMetadata that will return field type information.
        TypeMetadata typeMetadata = EasyMock.mock(TypeMetadata.class);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("FOO")).andReturn(NUMBER_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("BAR")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("HAT")).andReturn(NUMBER_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("BAT")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("HEN")).andReturn(NUMBER_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("VEE")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.replay(typeMetadata);
        givenTypeMetadata(typeMetadata);

        expectMessage("Numeric values supplied for non-numeric field(s): BAR, BAT, VEE");

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
