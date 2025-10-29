package datawave.query.rules;

import static datawave.query.Constants.ANY_FIELD;
import static datawave.query.Constants.NO_FIELD;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import datawave.microservice.query.Query;
import datawave.query.util.MetadataHelper;
import datawave.util.time.DateHelper;

@ExtendWith(EasyMockExtension.class)
public class FieldExistenceRuleTest extends ShardQueryRuleTest {

    private static final String beginDate = "20201231";
    private static final String endDate = "20251231";
    private static final Set<String> specialFields = Set.of(ANY_FIELD, NO_FIELD);

    @Mock
    private MetadataHelper helper;

    @Mock
    private Query settings;

    private final Set<String> existingFields = new HashSet<>();
    private final Set<String> foundFields = new HashSet<>();
    private final Set<String> missingFields = new HashSet<>();

    @BeforeAll
    public static void beforeClass() throws Exception {}

    @BeforeEach
    public void setUp() throws Exception {
        givenRuleName(RULE_NAME);
        givenMetadataHelper(helper);
        expectRuleName(RULE_NAME);
    }

    @AfterEach
    @Override
    void tearDown() {
        query = null;
        existingFields.clear();
        foundFields.clear();
        missingFields.clear();
    }

    /**
     * Test no non-existent fields or immaterial query subsets.
     */
    @Test
    void testNoIssuesFound() throws Exception {
        givenQuery("AGE == 'foo' || GENDER == 'bar'");

        givenExistingFields("AGE", "GENDER");
        expectFoundFields("AGE", "GENDER");

        assertResult();

        EasyMock.verify(settings, helper);
    }

    /**
     * Test the presence of a non-existent field.
     */
    @Test
    void testNonExistentFieldFound() throws Exception {
        givenQuery("AGE == 'foo' || GENDER == 'bar'");

        givenExistingFields("AGE");
        expectFoundFields("AGE", "GENDER");
        givenMissingFields("GENDER");

        expectMessage("Fields not found in data dictionary: GENDER");

        assertResult();

        EasyMock.verify(settings, helper);
    }

    /**
     * Test the presence of an immaterial query subset.
     */
    @Test
    void testImmaterialFieldsFound() throws Exception {
        givenQuery("AGE == 'foo' || GENDER == 'bar'");

        givenExistingFields("AGE", "GENDER");
        expectFoundFields("AGE", "GENDER");
        givenMissingFields("AGE", "GENDER");

        expectMessage("Fields not ingested in the provided date range: [AGE, GENDER]");

        assertResult();

        EasyMock.verify(settings, helper);
    }

    /**
     * Test the presence of a non-existent field and an immaterial query subset.
     */
    @Test
    void testNonExistentFieldsAndImmaterialFieldsFound() throws Exception {
        givenQuery("NAME == 'hat' && (AGE == 'foo' || GENDER == 'bar')");

        givenExistingFields("AGE", "GENDER");
        expectFoundFields("AGE", "GENDER", "NAME");
        givenMissingFields("AGE", "GENDER");

        expectMessage("Fields not found in data dictionary: NAME");
        expectMessage("Fields not ingested in the provided date range: [AGE, GENDER]");

        assertResult();

        EasyMock.verify(settings, helper);
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToJexl();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        EasyMock.expect(settings.getBeginDate()).andReturn(DateHelper.parse(beginDate));
        EasyMock.expect(settings.getEndDate()).andReturn(DateHelper.parse(endDate));
        givenQuerySettings(settings);

        try {
            EasyMock.expect(helper.getAllFields(Collections.emptySet())).andReturn(existingFields);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }

        if (!foundFields.isEmpty()) {
            EasyMock.expect(helper.getMissingFieldsInDateRange(foundFields, Collections.emptySet(), beginDate, endDate, specialFields))
                            .andReturn(missingFields);
        }

        EasyMock.replay(settings, helper);

        FieldExistenceRule rule = new FieldExistenceRule(ruleName);
        rule.setSpecialFields(specialFields);
        return rule;
    }

    private void givenExistingFields(String... fields) {
        this.existingFields.addAll(Arrays.asList(fields));
    }

    private void expectFoundFields(String... fields) {
        this.foundFields.addAll(Arrays.asList(fields));
    }

    private void givenMissingFields(String... fields) {
        this.missingFields.addAll(Arrays.asList(fields));
    }
}
