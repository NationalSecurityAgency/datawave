package datawave.query.jexl.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Due to the expansive nature of {@link EvaluationPhaseFilterFunctions}, tests for individual methods are encapsulated within their own test suites represented
 * by the nested classes found herein. The {@link Enclosed} runner will run all tests within these test suites.
 */
@RunWith(Enclosed.class)
public class EvaluationPhaseFilterFunctionsTest {
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#occurrence(Object, int)} and {@link EvaluationPhaseFilterFunctions#occurrence(Object, String, int)}.
     */
    public static class OccurrenceSingularValueTests {
        
        private static final Object singularFieldValue = new Object();
        private static final List<Object> listOfThreeValues = Lists.newArrayList(new Object(), new Object(), new Object());
        
        private Object fieldValue;
        private String operator;
        
        // Verify comparison for operator <.
        @Test
        public void testLessThanOperator() {
            givenOperator(" < ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForOperator(1));
            assertTrue(resultForOperator(2));
            
            // Results in count of 1.
            givenFieldValue(singularFieldValue);
            assertFalse(resultForOperator(1));
            assertTrue(resultForOperator(2));
            
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForOperator(3));
            assertTrue(resultForOperator(4));
        }
        
        // Verify comparison for operator <=.
        @Test
        public void testLessThanEqualsOperator() {
            givenOperator(" <= ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertTrue(resultForOperator(2));
            
            // Results in count of 1.
            givenFieldValue(singularFieldValue);
            assertFalse(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertTrue(resultForOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForOperator(2));
            assertTrue(resultForOperator(3));
            assertTrue(resultForOperator(4));
        }
        
        // Verify comparison for operator ==.
        @Test
        public void testDoubleEqualsOperator() {
            givenOperator(" == ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertFalse(resultForOperator(2));
            
            // Results in count of 1.
            givenFieldValue(singularFieldValue);
            assertFalse(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertFalse(resultForOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForOperator(2));
            assertTrue(resultForOperator(3));
            assertFalse(resultForOperator(4));
        }
        
        // Verify comparison for operator =.
        @Test
        public void testSingleEqualsOperator() {
            givenOperator(" = ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertFalse(resultForOperator(2));
            
            // Results in count of 1.
            givenFieldValue(singularFieldValue);
            assertFalse(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertFalse(resultForOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForOperator(2));
            assertTrue(resultForOperator(3));
            assertFalse(resultForOperator(4));
        }
        
        // Verify comparison for operator >=.
        @Test
        public void testGreaterThanEqualsOperator() {
            givenOperator(" >= ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertTrue(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertFalse(resultForOperator(2));
            
            // Results in count of 1.
            givenFieldValue(singularFieldValue);
            assertTrue(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertFalse(resultForOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertTrue(resultForOperator(2));
            assertTrue(resultForOperator(3));
            assertFalse(resultForOperator(4));
        }
        
        // Verify comparison for operator >.
        @Test
        public void testGreaterThanOperator() {
            givenOperator(" > ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertTrue(resultForOperator(0));
            assertFalse(resultForOperator(1));
            
            // Results in count of 1.
            givenFieldValue(singularFieldValue);
            assertTrue(resultForOperator(0));
            assertFalse(resultForOperator(1));
            
            givenFieldValue(listOfThreeValues);
            assertTrue(resultForOperator(2));
            assertFalse(resultForOperator(3));
        }
        
        // Verify comparison for operator >.
        @Test
        public void testNotEqualOperator() {
            givenOperator(" != ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertTrue(resultForOperator(0));
            assertFalse(resultForOperator(1));
            assertTrue(resultForOperator(2));
            
            // Results in count of 1.
            givenFieldValue(singularFieldValue);
            assertTrue(resultForOperator(0));
            assertFalse(resultForOperator(1));
            assertTrue(resultForOperator(2));
            
            givenFieldValue(listOfThreeValues);
            assertTrue(resultForOperator(2));
            assertFalse(resultForOperator(3));
            assertTrue(resultForOperator(4));
        }
        
        // Verify an exception is thrown when an invalid operator is given.
        @Test
        public void testInvalidOperator() {
            givenFieldValue(null);
            givenOperator("~=");
            
            assertThatIllegalArgumentException().isThrownBy(() -> resultForOperator(0)).withMessage("cannot use ~= in this equation");
        }
        
        // Verify that the default operator is ==.
        @Test
        public void testDefaultOperator() {
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForDefaultOperator(0));
            assertTrue(resultForDefaultOperator(1));
            assertFalse(resultForDefaultOperator(2));
            
            // Results in count of 1.
            givenFieldValue(singularFieldValue);
            assertFalse(resultForDefaultOperator(0));
            assertTrue(resultForDefaultOperator(1));
            assertFalse(resultForDefaultOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForDefaultOperator(2));
            assertTrue(resultForDefaultOperator(3));
            assertFalse(resultForDefaultOperator(4));
        }
        
        private void givenFieldValue(Object fieldValue) {
            this.fieldValue = fieldValue;
        }
        
        private void givenOperator(String operator) {
            this.operator = operator;
        }
        
        private boolean resultForOperator(int count) {
            return EvaluationPhaseFilterFunctions.occurrence(fieldValue, operator, count);
        }
        
        private boolean resultForDefaultOperator(int count) {
            return EvaluationPhaseFilterFunctions.occurrence(fieldValue, count);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#occurrence(Iterable, String, int)} and {@link EvaluationPhaseFilterFunctions#occurrence(Iterable, int)}.
     */
    public static class OccurrenceIterableValueTests {
        
        private static final List<Object> listOfThreeValues = Lists.newArrayList(new Object(), new Object(), new Object());
        
        private Iterable<?> fieldValue;
        private String operator;
        
        // Verify comparison for operator <.
        @Test
        public void testLessThanOperator() {
            givenOperator(" < ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForOperator(1));
            assertTrue(resultForOperator(2));
            
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForOperator(3));
            assertTrue(resultForOperator(4));
        }
        
        // Verify comparison for operator <=.
        @Test
        public void testLessThanEqualsOperator() {
            givenOperator(" <= ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertTrue(resultForOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForOperator(2));
            assertTrue(resultForOperator(3));
            assertTrue(resultForOperator(4));
        }
        
        // Verify comparison for operator ==.
        @Test
        public void testDoubleEqualsOperator() {
            givenOperator(" == ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertFalse(resultForOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForOperator(2));
            assertTrue(resultForOperator(3));
            assertFalse(resultForOperator(4));
        }
        
        // Verify comparison for operator =.
        @Test
        public void testSingleEqualsOperator() {
            givenOperator(" = ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertFalse(resultForOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForOperator(2));
            assertTrue(resultForOperator(3));
            assertFalse(resultForOperator(4));
        }
        
        // Verify comparison for operator >=.
        @Test
        public void testGreaterThanEqualsOperator() {
            givenOperator(" >= ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertTrue(resultForOperator(0));
            assertTrue(resultForOperator(1));
            assertFalse(resultForOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertTrue(resultForOperator(2));
            assertTrue(resultForOperator(3));
            assertFalse(resultForOperator(4));
        }
        
        // Verify comparison for operator >.
        @Test
        public void testGreaterThanOperator() {
            givenOperator(" > ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertTrue(resultForOperator(0));
            assertFalse(resultForOperator(1));
            
            givenFieldValue(listOfThreeValues);
            assertTrue(resultForOperator(2));
            assertFalse(resultForOperator(3));
        }
        
        // Verify comparison for operator >.
        @Test
        public void testNotEqualOperator() {
            givenOperator(" != ");
            
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertTrue(resultForOperator(0));
            assertFalse(resultForOperator(1));
            assertTrue(resultForOperator(2));
            
            givenFieldValue(listOfThreeValues);
            assertTrue(resultForOperator(2));
            assertFalse(resultForOperator(3));
            assertTrue(resultForOperator(4));
        }
        
        // Verify an exception is thrown when an invalid operator is given.
        @Test
        public void testInvalidOperator() {
            givenFieldValue(null);
            givenOperator("~=");
            
            assertThatIllegalArgumentException().isThrownBy(() -> resultForOperator(0)).withMessage("cannot use ~= in this equation");
        }
        
        // Verify that the default operator is ==.
        @Test
        public void testDefaultOperator() {
            // Defaults to a count of 1.
            givenFieldValue(null);
            assertFalse(resultForDefaultOperator(0));
            assertTrue(resultForDefaultOperator(1));
            assertFalse(resultForDefaultOperator(2));
            
            // Results in count of 3.
            givenFieldValue(listOfThreeValues);
            assertFalse(resultForDefaultOperator(2));
            assertTrue(resultForDefaultOperator(3));
            assertFalse(resultForDefaultOperator(4));
        }
        
        // These tests show examples of odd behavior that is not clearly documented, and is intended to be reviewed in order to determine what the correct
        // behavior should be.
        @Test
        public void testsToShowOddBehavior() {
            givenOperator("==");
            List<Object> values = new ArrayList<>();
            givenFieldValue(values);
            
            // If the list contains a mixture of objects and ValueTuples with a Type as its second, the count of the last type will be returned, and will not
            // include the count for objects seen before it.
            values.add(new Object());
            values.add(new Object());
            values.add(new Object());
            values.add(toValueTuple("FOO.1,BAR,bar", NoOpType::new));
            values.add(toValueTuple("FOO.1,BAR,bar", NoOpType::new));
            values.add(toValueTuple("FOO.1,BAR,bar", LcNoDiacriticsType::new));
            assertTrue(resultForOperator(1)); // The total number of LcNoDiacriticsType instances seen (1).
            
            // Similar to the case above, but this time there are objects in the list after the last type seen. The count for these objects will be added to the
            // count of the last type seen.
            values.clear();
            values.add(toValueTuple("FOO.1,BAR,bar", LcNoDiacriticsType::new));
            values.add(toValueTuple("FOO.1,BAR,bar", NoOpType::new));
            values.add(toValueTuple("FOO.1,BAR,bar", NoOpType::new));
            values.add(new Object());
            values.add(new Object());
            values.add(new Object());
            assertTrue(resultForOperator(5)); // The count of the last type, NoOpType, seen (2) plus the number of non-typed objects seen after that (3).
            
            // Given a list with no ValueTuples with a Type as its second, the count will be equivalent to the list size.
            values.clear();
            values.add(new Object());
            values.add(new Object());
            values.add(toNonTypedValueTuple("Foo.1,BAR,bar"));
            values.add(toNonTypedValueTuple("Foo.2,BAT,bat"));
            values.add(toNonTypedValueTuple("Foo.3,FOO,foo"));
            values.add(toNonTypedValueTuple("Foo.4,GRUMP,grump"));
            values.add(new Object());
            values.add(new Object());
            assertTrue(resultForOperator(8)); // Returns the total number of objects seen (8).
        }
        
        private void givenFieldValue(Iterable<?> fieldValue) {
            this.fieldValue = fieldValue;
        }
        
        private void givenOperator(String operator) {
            this.operator = operator;
        }
        
        private boolean resultForOperator(int count) {
            return EvaluationPhaseFilterFunctions.occurrence(fieldValue, operator, count);
        }
        
        private boolean resultForDefaultOperator(int count) {
            return EvaluationPhaseFilterFunctions.occurrence(fieldValue, count);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#isNotNull(Object)}.
     */
    public static class IsNotNullTests {
        
        private Object fieldValue;
        
        // Verify that a null object returns an empty set.
        @Test
        public void testNullValue() {
            givenFieldValue(null);
            assertThat(result()).isEmpty();
        }
        
        // Verify that a non-null value tuple returns a set with that value tuple.
        @Test
        public void testNonNullValue() {
            ValueTuple valueTuple = toValueTuple("FOO.1,BAR,bar");
            givenFieldValue(valueTuple);
            assertThat(result()).containsExactly(valueTuple);
        }
        
        // Verify that an empty collection returns an empty set.
        @Test
        public void testEmptyCollection() {
            givenFieldValue(Collections.emptySet());
            assertThat(result()).isEmpty();
        }
        
        // Verify that a non-empty collection of value tuples returns a set with the tuples.
        @Test
        public void givenNonEmptyCollection() {
            ValueTuple valueTuple1 = toValueTuple("FOO.1,BAR,bar");
            ValueTuple valueTuple2 = toValueTuple("FOO.1,ZOOM,zoom");
            givenFieldValue(Sets.newHashSet(valueTuple1, valueTuple2));
            assertThat(result()).containsExactlyInAnyOrder(valueTuple1, valueTuple2);
        }
        
        private void givenFieldValue(Object fieldValue) {
            this.fieldValue = fieldValue;
        }
        
        public Collection<ValueTuple> result() {
            return EvaluationPhaseFilterFunctions.isNotNull(fieldValue);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#isNull(Object)}
     */
    public static class IsNullTests {
        
        private Object fieldValue;
        
        // Verify that a null object is considered null.
        @Test
        public void testNullObject() {
            givenFieldValue(null);
            assertTrue(result());
        }
        
        // Verify that a non-null object that is not a collection is not considered null.
        @Test
        public void testNonNullNonCollection() {
            givenFieldValue(Sets.newHashSet(toValueTuple("FOO.1,BAR,bar")));
            assertFalse(result());
        }
        
        // Verify that an empty collection is considered null.
        @Test
        public void testEmptyCollection() {
            givenFieldValue(Collections.emptySet());
            assertTrue(result());
        }
        
        // Verify that non-empty collection is not considered null.
        @Test
        public void testNonEmptyCollection() {
            givenFieldValue(Sets.newHashSet(toValueTuple("FOO.1,BAR,bar")));
            assertFalse(result());
        }
        
        private void givenFieldValue(Object fieldValue) {
            this.fieldValue = fieldValue;
        }
        
        public boolean result() {
            return EvaluationPhaseFilterFunctions.isNull(fieldValue);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#excludeRegex(Object, String)} (Object, String)}.
     */
    public static class ExcludeRegexSingularValueTests {
        
        private Object fieldValue;
        private String regex;
        
        // Verify that given a null field value, that an empty set is returned.
        @Test
        public void testNullFieldValue() {
            fieldValue = null;
            givenRegex("ba.*");
            assertTrue(result());
        }
        
        @Test
        public void testCaseSensitiveMatching() {
            givenRegex("ba.*");
            
            // Verify that the regex matches against the value (second) portion of the value tuple.
            givenFieldValue(toValueTuple("FOO.1,BAR,boom"));
            assertFalse(result());
            
            // Verify that the regex matches against the normalized value (third) portion of the value tuple.
            givenFieldValue(toValueTuple("FOO.1,BOOM,bar"));
            assertFalse(result());
            
            // Verify that the regex does not match against the fieldName (first) portion of the value tuple.
            givenFieldValue(toValueTuple("BAR.1,BOOM,boom"));
            assertTrue(result());
        }
        
        @Test
        public void testCaseInsensitiveMatching() {
            givenRegex("(?i)ba.*(?-i)");
            
            // Verify that the regex matches against the value (second) portion of the value tuple.
            givenFieldValue(toValueTuple("FOO.1,BAR,boom"));
            assertFalse(result());
            
            // Verify that the regex does not match against the normalized value (third) portion of the value tuple.
            givenFieldValue(toValueTuple("FOO.1,BOOM,BAR"));
            assertTrue(result());
            
            // Verify that the regex does not match against the fieldName (first) portion of the value tuple.
            givenFieldValue(toValueTuple("BAR.1,BOOM,boom"));
            assertTrue(result());
        }
        
        private void givenFieldValue(Object fieldValue) {
            this.fieldValue = fieldValue;
        }
        
        private void givenRegex(String regex) {
            this.regex = regex;
        }
        
        private boolean result() {
            return EvaluationPhaseFilterFunctions.excludeRegex(fieldValue, regex);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#excludeRegex(Object, String)} (Iterable, String)}.
     */
    public static class ExcludeRegexIterableValueTests {
        
        private Iterable<Object> fieldValues;
        private String regex;
        
        @Test
        public void testNullFieldValues() {
            fieldValues = null;
            givenRegex("ba.*");
            assertTrue(result());
        }
        
        // Verify that the regex is only matched against the value and the normalized value for a case-sensitive regex.
        @Test
        public void testCaseSensitiveMatching() {
            givenRegex("ba.*");
            
            ValueTuple fieldNameMatch = toValueTuple("BAR.1,BOOM,boom");
            ValueTuple valueMatch = toValueTuple("FOO.1,BAR,boom");
            ValueTuple normalizedValueMatch = toValueTuple("FOO.1,BOOM,bar");
            
            // Arrange the value tuples so that the value will be matched first.
            givenFieldValues(fieldNameMatch, valueMatch, normalizedValueMatch);
            
            // Verify that the value was found as a match.
            assertFalse(result());
            
            // Rearrange the value tuples so that the normalized value will be matched first.
            givenFieldValues(normalizedValueMatch, valueMatch, fieldNameMatch);
            
            // Assert that the normalized value was found as a match.
            assertFalse(result());
            
            // Test a regex that should not match against any of the results.
            givenRegex("nomatch.*");
            assertTrue(result());
        }
        
        // Verify that the regex is only matched against the value for a case-insensitive regex.
        @Test
        public void testCaseInsensitiveMatching() {
            givenRegex("(?i)ba.*(?-i)");
            
            ValueTuple fieldNameMatch = toValueTuple("BAR.1,BOOM,boom");
            ValueTuple valueMatch = toValueTuple("FOO.1,BAR,boom");
            ValueTuple normalizedValueMatch = toValueTuple("FOO.1,BOOM,bar");
            givenFieldValues(fieldNameMatch, valueMatch, normalizedValueMatch);
            
            assertFalse(result());
        }
        
        // Verify that null values are skipped over.
        @Test
        public void testIterableWithNullElements() {
            givenRegex("(?i)ba.*(?-i)");
            
            ValueTuple valueMatch = toValueTuple("FOO.1,BAR,boom");
            givenFieldValues(null, valueMatch, null);
            
            assertFalse(result());
        }
        
        private void givenFieldValues(Object... values) {
            this.fieldValues = Lists.newArrayList(values);
        }
        
        private void givenRegex(String regex) {
            this.regex = regex;
        }
        
        private boolean result() {
            return EvaluationPhaseFilterFunctions.excludeRegex(fieldValues, regex);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#includeRegex(Object, String)}.
     */
    public static class IncludeRegexSingularValueTests {
        
        private Object fieldValue;
        private String regex;
        
        // Verify that given a null field value, that an empty set is returned.
        @Test
        public void testNullFieldValue() {
            fieldValue = null;
            givenRegex("ba.*");
            assertThat(result()).isEmpty();
        }
        
        @Test
        public void testCaseSensitiveMatching() {
            givenRegex("ba.*");
            
            // Verify that the regex matches against the value (second) portion of the value tuple.
            ValueTuple value = toValueTuple("FOO.1,BAR,boom");
            givenFieldValue(value);
            assertThat(result()).containsExactly(value);
            
            // Verify that the regex matches against the normalized value (third) portion of the value tuple.
            value = toValueTuple("FOO.1,BOOM,bar");
            givenFieldValue(value);
            assertThat(result()).containsExactly(value);
            
            // Verify that the regex does not match against the fieldName (first) portion of the value tuple.
            givenFieldValue(toValueTuple("BAR.1,BOOM,boom"));
            assertThat(result()).isEmpty();
        }
        
        @Test
        public void testCaseInsensitiveMatching() {
            givenRegex("(?i)ba.*(?-i)");
            
            // Verify that the regex matches against the value (second) portion of the value tuple.
            ValueTuple value = toValueTuple("FOO.1,BAR,boom");
            givenFieldValue(value);
            assertThat(result()).containsExactly(value);
            
            // Verify that the regex does not match against the normalized value (third) portion of the value tuple.
            givenFieldValue(toValueTuple("FOO.1,BOOM,BAR"));
            assertThat(result()).isEmpty();
            
            // Verify that the regex does not match against the fieldName (first) portion of the value tuple.
            givenFieldValue(toValueTuple("BAR.1,BOOM,boom"));
            assertThat(result()).isEmpty();
        }
        
        private void givenFieldValue(Object fieldValue) {
            this.fieldValue = fieldValue;
        }
        
        private void givenRegex(String regex) {
            this.regex = regex;
        }
        
        private FunctionalSet<ValueTuple> result() {
            return EvaluationPhaseFilterFunctions.includeRegex(fieldValue, regex);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#includeRegex(Iterable, String)}.
     */
    public static class IncludeRegexIterableValueTests {
        
        private Iterable<Object> fieldValues;
        private String regex;
        
        @Test
        public void testNullFieldValues() {
            fieldValues = null;
            givenRegex("ba.*");
            assertThat(result()).isEmpty();
        }
        
        // Verify that the regex is only matched against the value and the normalized value for a case-sensitive regex.
        @Test
        public void testCaseSensitiveMatching() {
            givenRegex("ba.*");
            
            ValueTuple fieldNameMatch = toValueTuple("BAR.1,BOOM,boom");
            ValueTuple valueMatch = toValueTuple("FOO.1,BAR,boom");
            ValueTuple normalizedValueMatch = toValueTuple("FOO.1,BOOM,bar");
            
            // Arrange the value tuples so that the value will be matched first.
            givenFieldValues(fieldNameMatch, valueMatch, normalizedValueMatch);
            
            // Verify that the value was found as a match.
            assertThat(result()).containsExactly(valueMatch);
            
            // Rearrange the value tuples so that the normalized value will be matched first.
            givenFieldValues(normalizedValueMatch, valueMatch, fieldNameMatch);
            
            // Assert that the normalized value was found as a match.
            assertThat(result()).containsExactly(normalizedValueMatch);
        }
        
        // Verify that the regex is only matched against the value for a case-insensitive regex.
        @Test
        public void testCaseInsensitiveMatching() {
            givenRegex("(?i)ba.*(?-i)");
            
            ValueTuple fieldNameMatch = toValueTuple("BAR.1,BOOM,boom");
            ValueTuple valueMatch = toValueTuple("FOO.1,BAR,boom");
            ValueTuple normalizedValueMatch = toValueTuple("FOO.1,BOOM,bar");
            givenFieldValues(fieldNameMatch, valueMatch, normalizedValueMatch);
            
            assertThat(result()).containsExactly(valueMatch);
        }
        
        // Verify that null values are skipped over.
        @Test
        public void testIterableWithNullElements() {
            givenRegex("(?i)ba.*(?-i)");
            
            ValueTuple valueMatch = toValueTuple("FOO.1,BAR,boom");
            givenFieldValues(null, valueMatch, null);
            
            assertThat(result()).containsExactly(valueMatch);
        }
        
        private void givenFieldValues(Object... values) {
            this.fieldValues = Lists.newArrayList(values);
        }
        
        private void givenRegex(String regex) {
            this.regex = regex;
        }
        
        private FunctionalSet<ValueTuple> result() {
            return EvaluationPhaseFilterFunctions.includeRegex(fieldValues, regex);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#getAllMatches(Object, String)}.
     */
    public static class GetAllMatchesSingularValueTests {
        
        private Object fieldValue;
        private String regex;
        
        // Verify that given a null field value, that an empty set is returned.
        @Test
        public void testNullFieldValue() {
            givenFieldValue(null);
            givenRegex("ba.*");
            assertThat(result()).isEmpty();
        }
        
        @Test
        public void testCaseSensitiveMatching() {
            givenRegex("ba.*");
            
            // Verify that the regex matches against the value (second) portion of the value tuple.
            ValueTuple value = toValueTuple("FOO.1,BAR,boom");
            givenFieldValue(value);
            assertThat(result()).containsExactly(value);
            
            // Verify that the regex matches against the normalized value (third) portion of the value tuple.
            value = toValueTuple("FOO.1,BOOM,bar");
            givenFieldValue(value);
            assertThat(result()).containsExactly(value);
            
            // Verify that the regex does not match against the fieldName (first) portion of the value tuple.
            givenFieldValue(toValueTuple("BAR.1,BOOM,boom"));
            assertThat(result()).isEmpty();
        }
        
        @Test
        public void testCaseInsensitiveMatching() {
            givenRegex("(?i)ba.*(?-i)");
            
            // Verify that the regex matches against the value (second) portion of the value tuple.
            ValueTuple value = toValueTuple("FOO.1,BAR,boom");
            givenFieldValue(value);
            assertThat(result()).containsExactly(value);
            
            // Verify that the regex does not match against the normalized value (third) portion of the value tuple.
            givenFieldValue(toValueTuple("FOO.1,BOOM,BAR"));
            assertThat(result()).isEmpty();
            
            // Verify that the regex does not match against the fieldName (first) portion of the value tuple.
            givenFieldValue(toValueTuple("BAR.1,BOOM,boom"));
            assertThat(result()).isEmpty();
        }
        
        private void givenFieldValue(Object fieldValue) {
            this.fieldValue = fieldValue;
        }
        
        private void givenRegex(String regex) {
            this.regex = regex;
        }
        
        private FunctionalSet<ValueTuple> result() {
            return EvaluationPhaseFilterFunctions.getAllMatches(fieldValue, regex);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#getAllMatches(Iterable, String)}.
     */
    public static class GetAllMatchesIterableValueTests {
        
        private Iterable<Object> fieldValues;
        private String regex;
        
        @Test
        public void testNullFieldValues() {
            fieldValues = null;
            givenRegex("ba.*");
            assertThat(result()).isEmpty();
        }
        
        // Verify that the regex is only matched against the value and the normalized value for a case-sensitive regex.
        @Test
        public void testCaseSensitiveMatching() {
            givenRegex("ba.*");
            
            ValueTuple fieldNameMatch = toValueTuple("BAR.1,BOOM,boom");
            ValueTuple valueMatch = toValueTuple("FOO.1,BAR,boom");
            ValueTuple normalizedValueMatch = toValueTuple("FOO.1,BOOM,bar");
            
            givenFieldValues(fieldNameMatch, valueMatch, normalizedValueMatch);
            
            // Verify that the value and normalized value were found as matches.
            assertThat(result()).containsExactlyInAnyOrder(valueMatch, normalizedValueMatch);
        }
        
        // Verify that the regex is only matched against the value for a case-insensitive regex.
        @Test
        public void testCaseInsensitiveMatching() {
            givenRegex("(?i)ba.*(?-i)");
            
            ValueTuple fieldNameMatch = toValueTuple("BAR.1,BOOM,boom");
            ValueTuple firstValueMatch = toValueTuple("FOO.1,BAR,boom");
            ValueTuple normalizedValueMatch = toValueTuple("FOO.1,BOOM,bar");
            ValueTuple secondValueMatch = toValueTuple("FOO.2,bar,groucho");
            givenFieldValues(fieldNameMatch, firstValueMatch, normalizedValueMatch, secondValueMatch);
            
            assertThat(result()).containsExactlyInAnyOrder(firstValueMatch, secondValueMatch);
        }
        
        // Verify that null values are skipped over.
        @Test
        public void testIterableWithNullElements() {
            givenRegex("(?i)ba.*(?-i)");
            
            ValueTuple valueMatch = toValueTuple("FOO.1,BAR,boom");
            givenFieldValues(null, valueMatch, null);
            
            assertThat(result()).containsExactly(valueMatch);
        }
        
        private void givenFieldValues(Object... values) {
            this.fieldValues = Lists.newArrayList(values);
        }
        
        private void givenRegex(String regex) {
            this.regex = regex;
        }
        
        private FunctionalSet<ValueTuple> result() {
            return EvaluationPhaseFilterFunctions.getAllMatches(fieldValues, regex);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#matchesAtLeastCountOf(Object[])}.
     */
    public static class MatchesAtLeastCountOfTests {
        
        private int minimum;
        private Iterable<Object> fieldValues;
        private String[] regexes;
        
        // Verify that when enough matches are found to meet the required minimum, that only the required minimum is returned, rather than all possible matches.
        @Test
        public void testMinimumMatchesFound() {
            givenMinimum(3);
            
            ValueTuple first = toValueTuple("STOOGE.1,MOE,moe");
            ValueTuple second = toValueTuple("STOOGE.2,LARRY,larry");
            ValueTuple third = toValueTuple("STOOGE.3,JOE,joe");
            ValueTuple fourth = toValueTuple("STOOGE.2,SHEMP,shemp");
            givenFieldValues(first, second, third, fourth);
            givenRegexes("MOE", "LARRY", "JOE", "SHEMP", "CURLEY JOE");
            
            assertThat(result()).containsExactlyInAnyOrder(first, second, third);
        }
        
        // Verify that when not enough matches are found to meet the required minimum, an empty set is returned.
        @Test
        public void testMinimumMatchesNotFound() {
            givenMinimum(4);
            givenFieldValues(toValueTuple("STOOGE.1,MOE,moe"), toValueTuple("STOOGE.2,LARRY,larry"), toValueTuple("STOOGE.3,GROUCHO,groucho"));
            givenRegexes("MOE", "LARRY", "JOE", "SHEMP", "CURLEY JOE");
            
            assertThat(result()).isEmpty();
        }
        
        private void givenMinimum(int minimum) {
            this.minimum = minimum;
        }
        
        private void givenFieldValues(Object... fieldValues) {
            this.fieldValues = Lists.newArrayList(fieldValues);
        }
        
        private void givenRegexes(String... regexes) {
            this.regexes = regexes;
        }
        
        private FunctionalSet<ValueTuple> result() {
            int argLength = 2 + regexes.length;
            Object[] args = new Object[argLength];
            args[0] = minimum;
            args[1] = fieldValues;
            System.arraycopy(regexes, 0, args, 2, regexes.length);
            return EvaluationPhaseFilterFunctions.matchesAtLeastCountOf(args);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#includeText(Object, String)}.
     */
    public static class IncludeTextSingularValue {
        
        private Object fieldValue;
        private String valueToMatch;
        
        // Verify that a null field value results in an empty set.
        @Test
        public void testNullValue() {
            givenFieldValue(null);
            givenValueToMatch("text");
            
            assertThat(result()).isEmpty();
        }
        
        // Verify that a set containing the hit term is returned when the text exactly matches the non-normalized value.
        @Test
        public void testMatch() {
            ValueTuple value = toValueTuple("FOO.1,BAR,boom");
            givenFieldValue(value);
            givenValueToMatch("BAR");
            
            assertThat(result()).containsExactly(value);
        }
        
        // Verify that an empty set is returned when the text does not exactly match the non-normalized value.
        @Test
        public void testNoMatch() {
            givenFieldValue(toValueTuple("FOO.1,BAR,boom"));
            givenValueToMatch("bar");
            
            assertThat(result()).isEmpty();
        }
        
        public void givenFieldValue(Object fieldValue) {
            this.fieldValue = fieldValue;
        }
        
        public void givenValueToMatch(String valueToMatch) {
            this.valueToMatch = valueToMatch;
        }
        
        public FunctionalSet<ValueTuple> result() {
            return EvaluationPhaseFilterFunctions.includeText(fieldValue, valueToMatch);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#includeText(Iterable, String)}.
     */
    public static class IncludeTextIterableValue {
        
        private Iterable<Object> fieldValues;
        private String valueToMatch;
        
        // Verify that a null iterable results in an empty set.
        @Test
        public void testNullIterable() {
            fieldValues = null;
            givenValueToMatch("FOO");
            
            assertThat(result()).isEmpty();
        }
        
        // Verify that a set is returned only with hit terms where the non-normalized value exactly match the text.
        @Test
        public void testNonNullIterable() {
            ValueTuple first = toValueTuple("FOO.1,BAR,boom");
            ValueTuple second = toValueTuple("FOO.1,boom,BAR");
            ValueTuple third = toValueTuple("BAR.1,foo,boom");
            ValueTuple fourth = toValueTuple("FOO.2,BAR,bar"); // Although this is a match, only the first match should be returned.
            givenFieldValues(first, second, third, fourth);
            givenValueToMatch("BAR");
            
            assertThat(result()).containsExactly(first);
        }
        
        public void givenFieldValues(Object... values) {
            this.fieldValues = Lists.newArrayList(values);
        }
        
        public void givenValueToMatch(String valueToMatch) {
            this.valueToMatch = valueToMatch;
        }
        
        public FunctionalSet<ValueTuple> result() {
            return EvaluationPhaseFilterFunctions.includeText(fieldValues, valueToMatch);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#getMatchToLeftOfPeriod(String, int)}.
     */
    public static class GetMatchToLeftOfPeriodTests {
        
        @SuppressWarnings("FieldCanBeLocal")
        private final String input = "first.second.third.fourth";
        private int position;
        
        // Verify that valid positions return the correct substrings.
        @Test
        public void testValidPositions() {
            givenPosition(0);
            assertResult("second.third");
            
            givenPosition(1);
            assertResult("second");
        }
        
        // Verify that an exception is thrown for an invalid position.
        @Test
        public void testInvalidPosition() {
            givenPosition(2);
            
            assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> assertResult("doesn't matter")).withMessage(
                            "Input second.third.fourth does not have a '.' at position " + position + " from the left.");
        }
        
        private void givenPosition(int position) {
            this.position = position;
        }
        
        private void assertResult(String expected) {
            assertThat(EvaluationPhaseFilterFunctions.getMatchToLeftOfPeriod(input, position)).isEqualTo(expected);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#getMatchToRightOfPeriod(String, int)}.
     */
    public static class GetMatchToRightOfPeriodTests {
        
        private final String input = "first.second.third.fourth";
        private int position;
        
        // Verify that valid positions return the correct substrings.
        @Test
        public void testValidPositions() {
            givenPosition(0);
            assertResult("fourth");
            
            givenPosition(1);
            assertResult("third.fourth");
            
            givenPosition(2);
            assertResult("second.third.fourth");
        }
        
        // Verify that an exception is thrown for an invalid position.
        @Test
        public void testInvalidPosition() {
            givenPosition(3);
            
            assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> assertResult("doesn't matter")).withMessage(
                            "Input " + input + " does not have a '.' at position " + position + " from the right.");
        }
        
        private void givenPosition(int position) {
            this.position = position;
        }
        
        private void assertResult(String expected) {
            assertThat(EvaluationPhaseFilterFunctions.getMatchToRightOfPeriod(input, position)).isEqualTo(expected);
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#betweenInclusive(long, long, long)}.
     */
    public static class BetweenInclusiveTests {
        
        @Test
        public void testLessThanRange() {
            assertFalse(EvaluationPhaseFilterFunctions.betweenInclusive(1L, 2L, 5L));
        }
        
        @Test
        public void testEqualToLeftOfRange() {
            assertTrue(EvaluationPhaseFilterFunctions.betweenInclusive(2L, 2L, 5L));
        }
        
        @Test
        public void testInBetweenRange() {
            assertTrue(EvaluationPhaseFilterFunctions.betweenInclusive(4L, 2L, 5L));
        }
        
        @Test
        public void testEqualToRightOfRange() {
            assertTrue(EvaluationPhaseFilterFunctions.betweenInclusive(5L, 2L, 5L));
        }
        
        @Test
        public void testGreaterThanRange() {
            assertFalse(EvaluationPhaseFilterFunctions.betweenInclusive(10L, 2L, 5L));
        }
    }
    
    /**
     * Tests for {@link EvaluationPhaseFilterFunctions#getTime(Object, boolean)}.
     */
    public static class GetTimeTests {
        
        private static final long ONEMILLISECOND = 1L;
        private static final long ONESECOND = TimeUnit.SECONDS.toMillis(1);
        private static final long ONEMINUTE = TimeUnit.MINUTES.toMillis(1);
        private static final long ONEHOUR = TimeUnit.HOURS.toMillis(1);
        private static final long ONEDAY = TimeUnit.DAYS.toMillis(1);
        
        private long expectedTime;
        
        // Verify that dates of supported formats are parsed correctly.
        @Test
        public void testSupportedFormats() throws ParseException {
            // Granular to month with default timezone GMT.
            givenExpectedTime(1633046400000L);
            assertTime(time("2021-Oct")); // yyyy-MMM
            assertTime(time("2021-October")); // yyyy-MMMM
            
            // Granular to day with default timezone GMT.
            givenExpectedTime(1633478400000L);
            assertTime(time("2021-10-06")); // yyyy-MM-dd
            assertTime(time("20211006")); // yyyyMMdd
            assertTime(time("10/06/2021")); // MM/dd/yyyy
            
            // Granular to hour with default timezone GMT.
            givenExpectedTime(1633536000000L);
            assertTime(time("2021100616")); // yyyyMMddHH
            
            // Granular to minute with default timezone GMT.
            givenExpectedTime(1633539060000L);
            assertTime(time("2021-10-06T16|51")); // yyyy-MM-dd'T'HH'|'mm
            assertTime(time("202110061651")); // yyyyMMddHHmm
            
            // Granular to second with default timezone GMT.
            givenExpectedTime(1633539094000L);
            assertTime(time("20211006 165134")); // yyyyMMdd HHmmss
            assertTime(time("2021-10-06 16:51:34")); // yyyy-MM-dd HH:mm:ss
            assertTime(time("2021-10-06T16:51:34Z")); // yyyy-MM-dd'T'HH':'mm':'ss'Z'
            assertTime(time("10/06/2021 16:51:34")); // MM'/'dd'/'yyyy HH':'mm':'ss
            assertTime(time("20211006165134")); // yyyyMMddHHmmss
            assertTime(time("20211006165134")); // yyyyMMddHHmmss
            assertTime(time("6 Oct 2021 16:51:34 GMT")); // d MMM yyyy HH:mm:ss 'GMT'
            
            // Granular to second with timezone EST.
            givenExpectedTime(1633557094000L);
            assertTime(time("2021-10-06 16:51:34 -0500")); // yyyy-MM-dd HH:mm:ss Z
            assertTime(time("2021-10-06 16:51:34EST")); // yyyy-MM-dd HH:mm:ssz
            assertTime(time("Wed Oct 06 16:51:34 EST 2021")); // EEE MMM dd HH:mm:ss zzz yyyy
            
            // Granular to millisecond with default timezone GMT.
            givenExpectedTime(1633539094215L);
            assertTime(time("2021-10-06 16:51:34.215")); // yyyy-MM-dd HH:mm:ss.S
            assertTime(time("2021-10-06 16:51:34.215")); // yyyy-MM-dd HH:mm:ss.SSS
            assertTime(time("20211006:16:51:34:215")); // yyyyMMdd:HH:mm:ss:SSS
            assertTime(time("2021-10-06T16:51:34.215Z")); // yyyy-MM-dd'T'HH':'mm':'ss'.'SSS'Z'
            
            // Granular to millisecond with timezone EST.
            givenExpectedTime(1633557094215L);
            assertTime(time("2021-10-06 16:51:34.215 -0500")); // yyyy-MM-dd HH:mm:ss.S Z
            assertTime(time("20211006:16:51:34:215-0500")); // yyyyMMdd:HH:mm:ss:SSSZ
            assertTime(time("2021-10-06 16:51:34.215 -0500")); // yyyy-MM-dd HH:mm:ss.SSS Z
        }
        
        // Verify the correct time is returned when dates are parsed with the intent of getting the next time based on the date format's granularity.
        @Test
        public void testNextTime() throws ParseException {
            // Granular to month with default timezone GMT. Should increase by one month. The actual total milliseconds added is dependent on the system's
            // timezone, so we must calculate the expected time first.
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(time("2021-Oct"));
            calendar.add(Calendar.MONTH, 1);
            
            givenExpectedTime(calendar.getTimeInMillis());
            assertTime(nextTime("2021-Oct")); // yyyy-MMM
            assertTime(nextTime("2021-October")); // yyyy-MMMM
            
            // Granular to day with default timezone GMT. Should increase by one day.
            givenExpectedTime(1633478400000L + ONEDAY);
            assertTime(nextTime("2021-10-06")); // yyyy-MM-dd
            assertTime(nextTime("20211006")); // yyyyMMdd
            assertTime(nextTime("10/06/2021")); // MM/dd/yyyy
            
            // Granular to hour with default timezone GMT. Should increase by one hour.
            givenExpectedTime(1633536000000L + ONEHOUR);
            assertTime(nextTime("2021100616")); // yyyyMMddHH
            
            // Granular to minute with default timezone GMT. Should increase by one minute.
            givenExpectedTime(1633539060000L + ONEMINUTE);
            assertTime(nextTime("2021-10-06T16|51")); // yyyy-MM-dd'T'HH'|'mm
            assertTime(nextTime("202110061651")); // yyyyMMddHHmm
            
            // Granular to second with default timezone GMT. Should increase by one second.
            givenExpectedTime(1633539094000L + ONESECOND);
            assertTime(nextTime("20211006 165134")); // yyyyMMdd HHmmss
            assertTime(nextTime("2021-10-06 16:51:34")); // yyyy-MM-dd HH:mm:ss
            assertTime(nextTime("2021-10-06T16:51:34Z")); // yyyy-MM-dd'T'HH':'mm':'ss'Z'
            assertTime(nextTime("10/06/2021 16:51:34")); // MM'/'dd'/'yyyy HH':'mm':'ss
            assertTime(nextTime("20211006165134")); // yyyyMMddHHmmss
            assertTime(nextTime("20211006165134")); // yyyyMMddHHmmss
            assertTime(nextTime("6 Oct 2021 16:51:34 GMT")); // d MMM yyyy HH:mm:ss 'GMT'
            
            // Granular to second with timezone EST. Should increase by one second.
            givenExpectedTime(1633557094000L + ONESECOND);
            assertTime(nextTime("2021-10-06 16:51:34 -0500")); // yyyy-MM-dd HH:mm:ss Z
            assertTime(nextTime("2021-10-06 16:51:34EST")); // yyyy-MM-dd HH:mm:ssz
            assertTime(nextTime("Wed Oct 06 16:51:34 EST 2021")); // EEE MMM dd HH:mm:ss zzz yyyy
            
            // Granular to millisecond with default timezone GMT. Should increase by one millisecond.
            givenExpectedTime(1633539094215L + ONEMILLISECOND);
            assertTime(nextTime("2021-10-06 16:51:34.215")); // yyyy-MM-dd HH:mm:ss.S
            assertTime(nextTime("2021-10-06 16:51:34.215")); // yyyy-MM-dd HH:mm:ss.SSS
            assertTime(nextTime("20211006:16:51:34:215")); // yyyyMMdd:HH:mm:ss:SSS
            assertTime(nextTime("2021-10-06T16:51:34.215Z")); // yyyy-MM-dd'T'HH':'mm':'ss'.'SSS'Z'
            
            // Granular to millisecond with timezone EST. Should increase by one millisecond.
            givenExpectedTime(1633557094215L + ONEMILLISECOND);
            assertTime(nextTime("2021-10-06 16:51:34.215 -0500")); // yyyy-MM-dd HH:mm:ss.S Z
            assertTime(nextTime("20211006:16:51:34:215-0500")); // yyyyMMdd:HH:mm:ss:SSSZ
            assertTime(nextTime("2021-10-06 16:51:34.215 -0500")); // yyyy-MM-dd HH:mm:ss.SSS Z
        }
        
        @Test
        public void testInvalidDate() {
            assertThatExceptionOfType(ParseException.class).isThrownBy(() -> nextTime("notavaliddate")).withMessage(
                            "Unable to parse value using known date formats: notavaliddate");
        }
        
        private void givenExpectedTime(long expectedTime) {
            this.expectedTime = expectedTime;
        }
        
        private void assertTime(long actual) {
            assertEquals(expectedTime, actual);
        }
        
        private long time(Object value) throws ParseException {
            return EvaluationPhaseFilterFunctions.getTime(value, false);
        }
        
        private long nextTime(Object value) throws ParseException {
            return EvaluationPhaseFilterFunctions.getTime(value, true);
        }
    }
    
    /**
     * Contains tests for {@link EvaluationPhaseFilterFunctions#compare(Object, String, String, Object)}.
     */
    public static class CompareTests {
        
        private static final ValueTuple A = toValueTuple("FOO.3,A,a");
        private static final ValueTuple B = toValueTuple("FOO.2,B,b");
        private static final ValueTuple C = toValueTuple("FOO.1,C,c");
        
        private Object field1;
        private Object field2;
        private String operator;
        private String compareMode;
        
        // Verify comparison for operator '<'.
        @Test
        public void testLessThanOperator() {
            givenOperator(" < ");
            
            // Any value in fields1 may satisfy the comparison.
            givenCompareModeAny();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is not less than itself.
            givenField1List(A);
            givenField2List(A);
            assertFalse(result());
            
            // C is not less than A or B.
            givenField1List(C);
            givenField2List(A, B);
            assertFalse(result());
            
            // A is less than B.
            givenField1List(A, C);
            givenField2List(B);
            assertTrue(result());
            
            // All values in fields1 must satisfy the comparison.
            givenCompareModeAll();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is not less than itself.
            givenField1List(A);
            givenField2List(A);
            assertFalse(result());
            
            // C is not less than A or B.
            givenField1List(C);
            givenField2List(A, B);
            assertFalse(result());
            
            // A is less than B, but C is not.
            givenField1List(A, C);
            givenField2List(B);
            assertFalse(result());
            
            // C is not less than C.
            givenField1List(A, B, C);
            givenField2List(C);
            assertFalse(result());
            
            // Both A and B are less than C.
            givenField1List(A, B);
            givenField2List(C);
            assertTrue(result());
        }
        
        // Verify comparison for operator '<='.
        @Test
        public void testLessThanOrEqualToOperator() {
            givenOperator(" <= ");
            
            // Any value in fields1 may satisfy the comparison.
            givenCompareModeAny();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is equal to itself.
            givenField1List(A);
            givenField2List(A);
            assertTrue(result());
            
            // C is not less than A or B.
            givenField1List(C);
            givenField2List(A, B);
            assertFalse(result());
            
            // A is less than B.
            givenField1List(A, C);
            givenField2List(B);
            assertTrue(result());
            
            // All values in fields1 must satisfy the comparison.
            givenCompareModeAll();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is less than itself.
            givenField1List(A);
            givenField2List(A);
            assertTrue(result());
            
            // C is not less than A or B.
            givenField1List(C);
            givenField2List(A, B);
            assertFalse(result());
            
            // A is less than B, but C is not.
            givenField1List(A, C);
            givenField2List(B);
            assertFalse(result());
            
            // A, B, and C are less than or equal to C.
            givenField1List(A, B, C);
            givenField2List(C);
            assertTrue(result());
            
            // Both A and B are less than C.
            givenField1List(A, B);
            givenField2List(C);
            assertTrue(result());
        }
        
        // Verify comparison for operator '>'.
        @Test
        public void testGreaterThanOperator() {
            givenOperator(" > ");
            
            // Any value in fields1 may satisfy the comparison.
            givenCompareModeAny();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is not greater than itself.
            givenField1List(A);
            givenField2List(A);
            assertFalse(result());
            
            // A is not greater than B or C.
            givenField1List(A);
            givenField2List(B, C);
            assertFalse(result());
            
            // C is greater than B.
            givenField1List(A, C);
            givenField2List(B);
            assertTrue(result());
            
            // All values in fields1 must satisfy the comparison.
            givenCompareModeAll();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is not greater than itself.
            givenField1List(A);
            givenField2List(A);
            assertFalse(result());
            
            // A is not greater than B or C.
            givenField1List(A);
            givenField2List(B, C);
            assertFalse(result());
            
            // C is greater than B, but A is not.
            givenField1List(A, C);
            givenField2List(B);
            assertFalse(result());
            
            // C is not greater than C.
            givenField1List(A, B, C);
            givenField2List(C);
            assertFalse(result());
            
            // Both B and C are greater than A.
            givenField1List(B, C);
            givenField2List(A);
            assertTrue(result());
        }
        
        // Verify comparison for operator '>='.
        @Test
        public void testGreaterThanEqualsOperator() {
            givenOperator(" >= ");
            
            // Any value in fields1 may satisfy the comparison.
            givenCompareModeAny();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is equal to itself.
            givenField1List(A);
            givenField2List(A);
            assertTrue(result());
            
            // A is not greater than B or C.
            givenField1List(A);
            givenField2List(B, C);
            assertFalse(result());
            
            // C is greater than B.
            givenField1List(A, C);
            givenField2List(B);
            assertTrue(result());
            
            // All values in fields1 must satisfy the comparison.
            givenCompareModeAll();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is equal to itself.
            givenField1List(A);
            givenField2List(A);
            assertTrue(result());
            
            // A is not greater than B or C.
            givenField1List(A);
            givenField2List(B, C);
            assertFalse(result());
            
            // C is greater than B, but A is not.
            givenField1List(A, C);
            givenField2List(B);
            assertFalse(result());
            
            // A, B, and C is are equal to or greater than A.
            givenField1List(A, B, C);
            givenField2List(A);
            assertTrue(result());
            
            // Both B and C are greater than A.
            givenField1List(B, C);
            givenField2List(A);
            assertTrue(result());
        }
        
        // Verify comparison for operator '='.
        @Test
        public void testEqualsOperator() {
            givenOperator(" = ");
            
            // Any value in fields1 may satisfy the comparison.
            givenCompareModeAny();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(true);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is equal to itself.
            givenField1List(A);
            givenField2List(A);
            assertTrue(result());
            
            // A is not equal to B or C.
            givenField1List(A);
            givenField2List(B, C);
            assertFalse(result());
            
            // B is equal to B.
            givenField1List(A, B);
            givenField2List(B);
            assertTrue(result());
            
            // All values in fields1 must satisfy the comparison.
            givenCompareModeAll();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(true);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is equal to itself.
            givenField1List(A);
            givenField2List(A);
            assertTrue(result());
            
            // C has a match, but A is not equal to B.
            givenField1List(A, C);
            givenField2List(B, C);
            assertFalse(result());
            
            // C and B have matches, but A does not.
            givenField1List(A, B, C);
            givenField2List(C, B);
            assertFalse(result());
            
            // Both B and C have matches.
            givenField1List(B, C);
            givenField2List(C, B);
            assertTrue(result());
        }
        
        // Verify comparison for operator '=='.
        @Test
        public void testDoubleEqualsOperator() {
            givenOperator(" == ");
            
            // Any value in fields1 may satisfy the comparison.
            givenCompareModeAny();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(true);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is equal to itself.
            givenField1List(A);
            givenField2List(A);
            assertTrue(result());
            
            // A is not equal to B or C.
            givenField1List(A);
            givenField2List(B, C);
            assertFalse(result());
            
            // B is equal to B.
            givenField1List(A, B);
            givenField2List(B);
            assertTrue(result());
            
            // All values in fields1 must satisfy the comparison.
            givenCompareModeAll();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(true);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(false);
            
            // A is equal to itself.
            givenField1List(A);
            givenField2List(A);
            assertTrue(result());
            
            // C has a match, but A is not equal to B.
            givenField1List(A, C);
            givenField2List(B, C);
            assertFalse(result());
            
            // C and B have matches, but A does not.
            givenField1List(A, B, C);
            givenField2List(C, B);
            assertFalse(result());
            
            // Both B and C have matches.
            givenField1List(B, C);
            givenField2List(C, B);
            assertTrue(result());
        }
        
        // Verify comparison for operator !=.
        @Test
        public void testNotEqualsOperator() {
            givenOperator(" != ");
            
            // Any value in fields1 may satisfy the comparison.
            givenCompareModeAny();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(true);
            
            // A is not equal to B.
            givenField1List(A);
            givenField2List(B);
            assertTrue(result());
            
            // A is equal to A.
            givenField1List(A);
            givenField2List(A);
            assertFalse(result());
            
            // A is not equal to B.
            givenField1List(A, B);
            givenField2List(B);
            assertTrue(result());
            
            // All values in fields1 must satisfy the comparison.
            givenCompareModeAll();
            
            // Verify that null or empty collection combinations always result in false.
            assertResultForNullAndEmptyFieldCombinationsIs(false);
            assertResultForNonEmptyAndEmptyFieldCombinationIs(true);
            
            // A is not equal to B.
            givenField1List(A);
            givenField2List(B);
            assertTrue(result());
            
            // C is found in both.
            givenField1List(A, C);
            givenField2List(B, C);
            assertFalse(result());
            
            // Neither An nor B is equal to C.
            givenField1List(A, B);
            givenField2List(C);
            assertTrue(result());
        }
        
        private void givenField1(Object field1) {
            this.field1 = field1;
        }
        
        private void givenField1List(Object... values) {
            givenField1(Lists.newArrayList(values));
        }
        
        private void givenField2(Object field2) {
            this.field2 = field2;
        }
        
        private void givenField2List(Object... values) {
            givenField2(Lists.newArrayList(values));
        }
        
        private void givenOperator(String operator) {
            this.operator = operator;
        }
        
        private void givenCompareModeAny() {
            this.compareMode = "ANY";
        }
        
        private void givenCompareModeAll() {
            this.compareMode = "ALL";
        }
        
        private boolean result() {
            return EvaluationPhaseFilterFunctions.compare(field1, operator, compareMode, field2);
        }
        
        // Verify the expected result for combinations of a null value and empty collections.
        private void assertResultForNullAndEmptyFieldCombinationsIs(boolean expected) {
            givenField1(null);
            givenField2(Collections.emptyList());
            assertEquals(expected, result());
            
            givenField1(Collections.emptyList());
            givenField2(null);
            assertEquals(expected, result());
        }
        
        // Verify the expected result for combinations of an empty and populated collections.
        private void assertResultForNonEmptyAndEmptyFieldCombinationIs(boolean expected) {
            givenField1List(A, B, C);
            givenField2(Collections.emptyList());
            assertEquals(expected, result());
            
            givenField1(Collections.emptyList());
            givenField2List(A, B, C);
            assertEquals(expected, result());
        }
    }
    
    /**
     * Contains tests for {@link EvaluationPhaseFilterFunctions#afterDate(Iterable, String)}
     */
    public static class AfterDateTests {
        @Test
        public void testAfterDateIterator() {
            FunctionalSet<ValueTuple> values = new FunctionalSet<>();
            values.add(new ValueTuple("DATE_FIELD", "2021-10-15", "2021-10-15", null));
            
            FunctionalSet<ValueTuple> result = EvaluationPhaseFilterFunctions.afterDate(values, "2021-10-14");
            
            assertNotNull(result);
            assertEquals(1, result.size());
            for (ValueTuple valueTuple : result) {
                Assert.assertEquals("2021-10-15", valueTuple.getValue());
            }
        }
        
        @Test
        public void testAfterDateFormatterIterator() {
            FunctionalSet<ValueTuple> values = new FunctionalSet<>();
            values.add(new ValueTuple("DATE_FIELD", "2021-10-15", "2021-10-15", null));
            
            FunctionalSet<ValueTuple> result = EvaluationPhaseFilterFunctions.afterDate(values, "2021-10-14", "yyyy-MM-dd");
            
            assertNotNull(result);
            assertEquals(1, result.size());
            for (ValueTuple valueTuple : result) {
                Assert.assertEquals("2021-10-15", valueTuple.getValue());
            }
        }
        
        @Test
        public void testAfterDateFormatter2Iterator() {
            FunctionalSet<ValueTuple> values = new FunctionalSet<>();
            values.add(new ValueTuple("DATE_FIELD", "2021-10-15", "2021-10-15", null));
            
            FunctionalSet<ValueTuple> result = EvaluationPhaseFilterFunctions.afterDate(values, "yyyy-MM-dd", "2021-10-14", "yyyy-MM-dd");
            
            assertNotNull(result);
            assertEquals(1, result.size());
            for (ValueTuple valueTuple : result) {
                Assert.assertEquals("2021-10-15", valueTuple.getValue());
            }
        }
    }
    
    /**
     * Contains tests for {@link EvaluationPhaseFilterFunctions#beforeDate(Iterable, String)}.
     */
    public static class BeforeDateTests {
        
        @Test
        public void testBeforeDateIterator() {
            FunctionalSet<ValueTuple> values = new FunctionalSet<>();
            values.add(new ValueTuple("DATE_FIELD", "2021-10-15", "2021-10-15", null));
            
            FunctionalSet<ValueTuple> result = EvaluationPhaseFilterFunctions.beforeDate(values, "2021-10-16");
            
            assertNotNull(result);
            assertEquals(1, result.size());
            for (ValueTuple valueTuple : result) {
                Assert.assertEquals("2021-10-15", valueTuple.getValue());
            }
        }
        
        @Test
        public void testBeforeDateFormatterIterator() {
            FunctionalSet<ValueTuple> values = new FunctionalSet<>();
            values.add(new ValueTuple("DATE_FIELD", "2021-10-15", "2021-10-15", null));
            
            FunctionalSet<ValueTuple> result = EvaluationPhaseFilterFunctions.beforeDate(values, "2021-10-16", "yyyy-MM-dd");
            
            assertNotNull(result);
            assertEquals(1, result.size());
            for (ValueTuple valueTuple : result) {
                Assert.assertEquals("2021-10-15", valueTuple.getValue());
            }
        }
        
        @Test
        public void testBeforeDateFormatter2Iterator() {
            FunctionalSet<ValueTuple> values = new FunctionalSet<>();
            values.add(new ValueTuple("DATE_FIELD", "2021-10-15", "2021-10-15", null));
            
            FunctionalSet<ValueTuple> result = EvaluationPhaseFilterFunctions.beforeDate(values, "yyyy-MM-dd", "2021-10-16", "yyyy-MM-dd");
            
            assertNotNull(result);
            assertEquals(1, result.size());
            for (ValueTuple valueTuple : result) {
                Assert.assertEquals("2021-10-15", valueTuple.getValue());
            }
        }
    }
    
    private static ValueTuple toValueTuple(String csv) {
        return toValueTuple(csv, LcNoDiacriticsType::new);
    }
    
    private static ValueTuple toValueTuple(String csv, Function<String,Type<String>> typeConstructor) {
        String[] tokens = csv.split(",");
        String field = tokens[0];
        Type<String> type = typeConstructor.apply(tokens[1]);
        TypeAttribute<String> typeAttribute = new TypeAttribute<>(type, new Key(), true);
        String normalized = tokens[2];
        return new ValueTuple(field, type, normalized, typeAttribute);
    }
    
    private static ValueTuple toNonTypedValueTuple(String csv) {
        String[] tokens = csv.split(",");
        String field = tokens[0];
        String normalized = tokens[2];
        PreNormalizedAttribute attribute = new PreNormalizedAttribute(normalized, null, false);
        return new ValueTuple(field, "second", normalized, attribute);
    }
}
