package datawave.query.rules;

import static datawave.query.Constants.ANY_FIELD;
import static datawave.query.Constants.NO_FIELD;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import datawave.microservice.query.Query;
import datawave.query.util.MetadataHelper;
import datawave.util.time.DateHelper;

@ExtendWith(MockitoExtension.class)
public class FieldExistenceRuleTest extends ShardQueryRuleTest {

    private final Map<List<?>,Integer> expectedCallCounts = new HashMap<>();
    private final List<Executable> interactionChecks = new ArrayList<>();

    private static final String beginDate = "20201231";
    private static final String endDate = "20251231";
    private static final Set<String> specialFields = Set.of(ANY_FIELD, NO_FIELD);
    private static final Set<String> hiddenFields = Set.of("HIDDEN_FIELD");

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

        assertAll(interactionChecks);

        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
        if (settings != null && mockingDetails(settings).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(settings));
        }
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

        assertAll(interactionChecks);

        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
        if (settings != null && mockingDetails(settings).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(settings));
        }
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

        expectMessage("There is no data in the following fields [AGE, GENDER] for the entire query date range 20201231 to 20251231.");

        assertResult();

        assertAll(interactionChecks);

        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
        if (settings != null && mockingDetails(settings).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(settings));
        }
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
        expectMessage("There is no data in the following fields [AGE, GENDER] for the entire query date range 20201231 to 20251231.");

        assertResult();

        assertAll(interactionChecks);

        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
        if (settings != null && mockingDetails(settings).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(settings));
        }
    }

    /**
     * Test a query with a hidden field.
     */
    @Test
    public void testHiddenFields() throws Exception {
        givenQuery("FOO == 'abc' || BAR =~ 'abc' || filter:includeRegex(SHENANIGANS, '45.8') || HIDDEN_FIELD == 'aa'");
        expectMessage("Fields not found in data dictionary: SHENANIGANS, HIDDEN_FIELD");

        givenExistingFields("FOO", "BAR");
        expectFoundFields("FOO", "BAR", "SHENANIGANS", "HIDDEN_FIELD");
        givenMissingFields("SHENANIGANS", "HIDDEN_FIELD");

        assertResult();

        assertAll(interactionChecks);

        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
        if (settings != null && mockingDetails(settings).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(settings));
        }
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToJexl();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        lenient().doReturn(DateHelper.parse(beginDate)).when(settings).getBeginDate();
        lenient().doReturn(DateHelper.parse(endDate)).when(settings).getEndDate();
        givenQuerySettings(settings);

        try {
            doReturn(hiddenFields).when(helper).getHiddenFields(Collections.emptySet());
            List<?> expectedCall1 = Arrays.asList(helper, "getHiddenFields", Collections.emptySet());
            expectedCallCounts.merge(expectedCall1, 1, Integer::sum);
            interactionChecks.add(() -> verify(helper, times(expectedCallCounts.get(expectedCall1))).getHiddenFields(Collections.emptySet()));
            doReturn(existingFields).when(helper).getAllFields(Collections.emptySet());
            List<?> expectedCall2 = Arrays.asList(helper, "getAllFields", Collections.emptySet());
            expectedCallCounts.merge(expectedCall2, 1, Integer::sum);
            interactionChecks.add(() -> verify(helper, times(expectedCallCounts.get(expectedCall2))).getAllFields(Collections.emptySet()));
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }

        if (!foundFields.isEmpty()) {
            doReturn(missingFields).when(helper).getMissingFieldsInDateRange(foundFields, Collections.emptySet(), beginDate, endDate, specialFields);
            List<?> expectedCall3 = Arrays.asList(helper, "getMissingFieldsInDateRange", foundFields, Collections.emptySet(), beginDate, endDate,
                            specialFields);
            expectedCallCounts.merge(expectedCall3, 1, Integer::sum);
            interactionChecks.add(() -> verify(helper, times(expectedCallCounts.get(expectedCall3))).getMissingFieldsInDateRange(foundFields,
                            Collections.emptySet(), beginDate, endDate, specialFields));
        }

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
