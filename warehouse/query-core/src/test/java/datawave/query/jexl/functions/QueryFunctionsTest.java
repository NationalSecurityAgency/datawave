package datawave.query.jexl.functions;

import com.google.common.collect.Lists;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Due to the expansive nature of {@link QueryFunctions}, tests for individual methods are encapsulated within their own test suites represented by the nested
 * classes found herein. The {@link Enclosed} runner will run all tests within these test suites.
 */
@RunWith(Enclosed.class)
public class QueryFunctionsTest {
    
    /**
     * Tests for {@link QueryFunctions#matchRegex(Object, String)}.
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
            return QueryFunctions.matchRegex(fieldValue, regex);
        }
    }
    
    /**
     * Tests for {@link QueryFunctions#includeText(Object, String)}.
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
            return QueryFunctions.includeText(fieldValue, valueToMatch);
        }
    }
    
    /**
     * Tests for {@link QueryFunctions#matchRegex(Iterable, String)}.
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
            return QueryFunctions.matchRegex(fieldValues, regex);
        }
    }
    
    /**
     * Tests for {@link QueryFunctions#includeText(Iterable, String)}.
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
            return QueryFunctions.includeText(fieldValues, valueToMatch);
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
}
