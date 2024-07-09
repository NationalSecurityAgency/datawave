package datawave.query.jexl.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.accumulo.core.data.Key;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;

@RunWith(Enclosed.class)
public class GroupingRequiredFilterFunctionsTest {

    private abstract static class BaseTest {
        protected final LcNoDiacriticsType lcNoDiacriticsType = new LcNoDiacriticsType();
        protected final NumberType numberType = new NumberType();

        protected final List<Object> args = new ArrayList<>();
        protected boolean normalizeFilterToRegex = true;

        @After
        public void tearDown() throws Exception {
            this.args.clear();
            this.normalizeFilterToRegex = true;
        }

        protected abstract void assertResult();

        protected void givenArg(Object arg) {
            this.args.add(arg);
        }

        protected void givenNormalizeFilterToRegex(boolean val) {
            this.normalizeFilterToRegex = val;
        }

        protected ValueTuple lcNoDiacriticsTuple(String field, String value) {
            return createTuple(field, value, LcNoDiacriticsType::new);
        }

        protected ValueTuple numberTypeTuple(String field, String value) {
            return createTuple(field, value, NumberType::new);
        }

        protected <T extends Comparable<T>> ValueTuple createTuple(String field, String value, Function<String,Type<T>> typeConstructor) {
            Type<T> type = typeConstructor.apply(value);
            TypeAttribute<T> attribute = new TypeAttribute<>(type, new Key(), true);
            return new ValueTuple(field, type, type.getNormalizedValue(), attribute);
        }

        protected String normalizeLcNoDiacriticsFilter(String filter) {
            return normalizeFilter(filter, lcNoDiacriticsType);
        }

        protected String normalizeNumberTypeFilter(String filter) {
            return normalizeFilter(filter, numberType);
        }

        protected String normalizeFilter(String filter, Type<?> type) {
            return normalizeFilterToRegex ? type.normalizeRegex(filter) : type.normalize(filter);
        }
    }

    /**
     * Contains tests for {@link GroupingRequiredFilterFunctions#atomValuesMatch(Object...)} and
     * {@link GroupingRequiredFilterFunctions#atomValuesMatch(Iterable[])}.
     */
    public static class AtomValuesMatchTest extends BaseTest {

        private final List<ValueTuple> expected = new ArrayList<>();
        private boolean iterableArgs = false;

        @After
        public void tearDown() throws Exception {
            super.tearDown();
            iterableArgs = false;
            expected.clear();
        }

        /**
         * Verify that for a vararg of value tuples, if we have matching values and matching grouping contexts in each tuple, that each match is returned.
         */
        @Test
        public void testMatchesFound() {
            // There should be three matches for 'bar', one in each arg.
            givenArg(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            givenArg(lcNoDiacriticsTuple("BETA.1", "BAR"));
            givenArg(lcNoDiacriticsTuple("GAMMA.1", "BAR"));

            expect(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            expect(lcNoDiacriticsTuple("BETA.1", "BAR"));
            expect(lcNoDiacriticsTuple("GAMMA.1", "BAR"));

            assertResult();
        }

        /**
         * Verify that when each argument is a singleton collection, that we are able to find matches across the collections.
         */
        @Test
        public void testMatchesFoundInSingletonCollectionArgs() {
            // There should be three matches for 'bar', one in each arg.
            givenArg(Collections.singleton(lcNoDiacriticsTuple("ALPHA.1", "BAR")));
            givenArg(Collections.singleton(lcNoDiacriticsTuple("BETA.1", "BAR")));
            givenArg(Collections.singleton(lcNoDiacriticsTuple("GAMMA.1", "BAR")));

            expect(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            expect(lcNoDiacriticsTuple("BETA.1", "BAR"));
            expect(lcNoDiacriticsTuple("GAMMA.1", "BAR"));

            assertResult();
        }

        /**
         * Verify that for a vararg of value tuples, if all the args have matching grouping context, but different values, that no matches are returned.
         */
        @Test
        public void testDifferingValues() {
            givenArg(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            givenArg(lcNoDiacriticsTuple("BETA.1", "BAR"));
            givenArg(lcNoDiacriticsTuple("GAMMA.1", "FOO")); // Different value.

            assertResult();
        }

        /**
         * Verify that for a vararg of value tuples, if all the args have matching grouping context, but different values, that no matches are returned.
         */
        @Test
        public void testDifferingValuesInSingletonCollectionArgs() {
            givenArg(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            givenArg(lcNoDiacriticsTuple("BETA.1", "BAR"));
            givenArg(lcNoDiacriticsTuple("GAMMA.1", "FOO")); // Different value.

            assertResult();
        }

        /**
         * Verify that for a vararg of value tuples, if one of the args has a matching value, but a different grouping context, that no matches are returned.
         */
        @Test
        public void testDifferingGroupingContexts() {
            givenArg(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            givenArg(lcNoDiacriticsTuple("BETA.2", "BAR")); // Different grouping context.
            givenArg(lcNoDiacriticsTuple("GAMMA.1", "BAR"));

            assertResult();
        }

        /**
         * Verify that for a vararg of iterables, that we are able to find matches between the iterables.
         */
        @Test
        public void testMatchesFoundAcrossIterables() {
            givenIterableArgs();

            // There should be 3 matches for 'bar' and 3 for 'baz'.
            // @formatter:off
            givenArg(Lists.newArrayList(
                            lcNoDiacriticsTuple("ALPHA.1", "BAR"),
                            lcNoDiacriticsTuple("ALPHA.2", "BAZ")));
            givenArg(Lists.newArrayList(
                            lcNoDiacriticsTuple("BETA.1", "BAR"),
                            lcNoDiacriticsTuple("BETA.2", "BAZ")));
            givenArg(Lists.newArrayList(
                            lcNoDiacriticsTuple("GAMMA.1", "BAR"),
                            lcNoDiacriticsTuple("GAMMA.2", "BAZ")));
            // @formatter:on

            expect(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            expect(lcNoDiacriticsTuple("ALPHA.2", "BAZ"));
            expect(lcNoDiacriticsTuple("BETA.1", "BAR"));
            expect(lcNoDiacriticsTuple("BETA.2", "BAZ"));
            expect(lcNoDiacriticsTuple("GAMMA.1", "BAR"));
            expect(lcNoDiacriticsTuple("GAMMA.2", "BAZ"));

            assertResult();
        }

        /**
         * Verify that if some fields have full matches across the different args, that the matches are returned.
         */
        @Test
        public void testPartialMatchesFound() {
            // There should be 3 matches for 'bar' only. There is no 'baz' in the third arg.
            // @formatter:off
            givenArg(Lists.newArrayList(
                            lcNoDiacriticsTuple("ALPHA.1", "BAR"),
                            lcNoDiacriticsTuple("ALPHA.2", "BAZ")));
            givenArg(Lists.newArrayList(
                            lcNoDiacriticsTuple("BETA.1", "BAR"),
                            lcNoDiacriticsTuple("BETA.2", "BAZ")));

            givenArg(lcNoDiacriticsTuple("GAMMA.1", "BAR"));
            // @formatter:on

            expect(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            expect(lcNoDiacriticsTuple("BETA.1", "BAR"));
            expect(lcNoDiacriticsTuple("GAMMA.1", "BAR"));

            assertResult();
        }

        /**
         * Verify that if any single-field arg does not have a match across all other args, that no matches are returned.
         */
        @Test
        public void testNoMatchesFoundForThirdArg() {
            // There should be no matches because 'biz', the only member in the 3rd arg, does not match anything in the other args.
            // @formatter:off
            givenArg(Lists.newArrayList(
                            lcNoDiacriticsTuple("ALPHA.1", "BAR"),
                            lcNoDiacriticsTuple("ALPHA.2", "BAZ")));
            givenArg(Lists.newArrayList(
                            lcNoDiacriticsTuple("BETA.1", "BAR"),
                            lcNoDiacriticsTuple("BETA.2", "BAZ")));
            givenArg(lcNoDiacriticsTuple("GAMMA.1", "BIZ"));
            // @formatter:on

            assertResult();
        }

        private void givenIterableArgs() {
            iterableArgs = true;
        }

        private void expect(ValueTuple tuple) {
            expected.add(tuple);
        }

        @Override
        protected void assertResult() {
            Collection<ValueTuple> valueTuples;
            if (iterableArgs) {
                // noinspection SuspiciousToArrayCall
                valueTuples = GroupingRequiredFilterFunctions.atomValuesMatch(args.toArray(new Iterable<?>[0]));
            } else {
                valueTuples = GroupingRequiredFilterFunctions.atomValuesMatch(args.toArray(new Object[0]));
            }
            Assertions.assertThat(valueTuples).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    /**
     * Contains tests for {@link GroupingRequiredFilterFunctions#matchesInGroup(Object...)}.
     */
    public static class MatchesInGroupTests extends BaseTest {

        private final List<ValueTuple> expected = new ArrayList<>();

        @After
        public void tearDown() throws Exception {
            super.tearDown();
            expected.clear();
        }

        @Test
        public void testMatchesFound() {
            givenArg(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            givenArg(normalizeLcNoDiacriticsFilter("bar"));
            givenArg(lcNoDiacriticsTuple("GAMMA.1", "baz"));
            givenArg(normalizeLcNoDiacriticsFilter("baz"));
            givenArg(numberTypeTuple("BETA.1", "2"));
            givenArg(normalizeNumberTypeFilter("2"));

            expect(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            expect(lcNoDiacriticsTuple("GAMMA.1", "baz"));
            expect(numberTypeTuple("BETA.1", "2"));

            assertResult();
        }

        @Test
        public void testMatchesFoundInSingletonCollections() {
            givenArg(Collections.singleton(lcNoDiacriticsTuple("ALPHA.1", "BAR")));
            givenArg(normalizeLcNoDiacriticsFilter("bar"));
            givenArg(Collections.singleton(lcNoDiacriticsTuple("GAMMA.1", "baz")));
            givenArg(normalizeLcNoDiacriticsFilter("baz"));
            givenArg(Collections.singleton(numberTypeTuple("BETA.1", "2")));
            givenArg(normalizeNumberTypeFilter("2"));

            expect(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            expect(lcNoDiacriticsTuple("GAMMA.1", "baz"));
            expect(numberTypeTuple("BETA.1", "2"));

            assertResult();
        }

        /**
         * Verify that regex matching works, including for number types.
         */
        @Test
        public void testMatchesFoundRegex() {
            givenNormalizeFilterToRegex(true);

            givenArg(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            givenArg(normalizeLcNoDiacriticsFilter("baR*"));
            givenArg(lcNoDiacriticsTuple("GAMMA.1", "baz"));
            givenArg(normalizeLcNoDiacriticsFilter("baz+"));
            givenArg(numberTypeTuple("BETA.1", "2"));
            givenArg(normalizeNumberTypeFilter("2*"));

            expect(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            expect(lcNoDiacriticsTuple("GAMMA.1", "baz"));
            expect(numberTypeTuple("BETA.1", "2"));

            assertResult();
        }

        private void expect(ValueTuple tuple) {
            expected.add(tuple);
        }

        @Override
        protected void assertResult() {
            @SuppressWarnings("unchecked")
            Collection<ValueTuple> valueTuples = (Collection<ValueTuple>) GroupingRequiredFilterFunctions.matchesInGroup(args.toArray(new Object[0]));
            Assertions.assertThat(valueTuples).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    /**
     * Contains tests for {@link GroupingRequiredFilterFunctions#getGroupsForMatchesInGroup(Object...)}.
     */
    public static class GetGroupsForMatchesInGroupsTest extends BaseTest {

        private final List<String> expected = new ArrayList<>();

        @After
        public void tearDown() throws Exception {
            super.tearDown();
            expected.clear();
        }

        @Test
        public void testGroupsFound() {
            givenArg(lcNoDiacriticsTuple("ALPHA.1", "BAR"));
            givenArg(normalizeLcNoDiacriticsFilter("bar"));
            givenArg(lcNoDiacriticsTuple("GAMMA.1", "BAZ"));
            givenArg(normalizeLcNoDiacriticsFilter("baz"));

            expect("1");

            assertResult();
        }

        @Test
        public void testGroupsFoundInSingletonCollectionArgs() {
            givenArg(Collections.singleton(lcNoDiacriticsTuple("ALPHA.1", "BAR")));
            givenArg(normalizeLcNoDiacriticsFilter("bar"));
            givenArg(Collections.singleton(lcNoDiacriticsTuple("GAMMA.1", "BAZ")));
            givenArg(normalizeLcNoDiacriticsFilter("baz"));

            expect("1");

            assertResult();
        }

        @Test
        public void testGroupsFoundInAllArgs() {
            givenArg(Lists.newArrayList(lcNoDiacriticsTuple("ALPHA.1", "BAR"), lcNoDiacriticsTuple("ALPHA.2", "BAR")));
            givenArg(normalizeLcNoDiacriticsFilter("bar"));
            givenArg(Lists.newArrayList(lcNoDiacriticsTuple("GAMMA.1", "BAZ")));
            givenArg(normalizeLcNoDiacriticsFilter("baz"));

            // there is only one match because, while 'bar' matches in both context group2 .1 and .2 for the 1st argument,
            // 'baz' matches only in context group .1 for the 2nd argument
            expect("1");
            expect("3");
        }

        private void expect(String group) {
            expected.add(group);
        }

        @Override
        protected void assertResult() {
            @SuppressWarnings("unchecked")
            Collection<String> groups = (Collection<String>) GroupingRequiredFilterFunctions.getGroupsForMatchesInGroup(args.toArray(new Object[0]));
            Assertions.assertThat(groups).containsExactlyInAnyOrderElementsOf(expected);
        }

    }

    public static class MatchesInGroupLeftTests extends BaseTest {
        private final List<ValueTuple> expected = new ArrayList<>();

        @After
        public void tearDown() throws Exception {
            super.tearDown();
            expected.clear();
        }

        @Test
        public void testDefaultIndex() {
            givenArg(lcNoDiacriticsTuple("NAME.grandparent_0.parent_0.child_1", "FREDO"));
            givenArg(normalizeLcNoDiacriticsFilter("fredo"));
            givenArg(lcNoDiacriticsTuple("NAME.grandparent_0.parent_0.child_0", "SANTINO"));
            givenArg(normalizeLcNoDiacriticsFilter("santino"));

            // Grouping context should have matched against parent_0.
            expect(lcNoDiacriticsTuple("NAME.grandparent_0.parent_0.child_1", "FREDO"));
            expect(lcNoDiacriticsTuple("NAME.grandparent_0.parent_0.child_0", "SANTINO"));

            assertResult();
        }

        @Test
        public void testIndexOfOne() {
            givenArg(lcNoDiacriticsTuple("NAME.grandparent_0.parent_0.child_1", "FREDO"));
            givenArg(normalizeLcNoDiacriticsFilter("fredo"));
            givenArg(lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_0", "SANTINO"));
            givenArg(normalizeLcNoDiacriticsFilter("santino"));
            givenArg(1);

            // Grouping context should have matched against grandparent_0.
            expect(lcNoDiacriticsTuple("NAME.grandparent_0.parent_0.child_1", "FREDO"));
            expect(lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_0", "SANTINO"));

            assertResult();
        }

        @Test
        public void testPartialMatch() {
            // @formatter:off
            givenArg(Lists.newArrayList(
                            lcNoDiacriticsTuple("NAME.grandparent_0.parent_0.child_1","FREDO"),
                            lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_1","FREDO")));
            givenArg(normalizeLcNoDiacriticsFilter("fredo"));
            givenArg(lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_0","SANTINO"));
            givenArg(normalizeLcNoDiacriticsFilter("SANTINO"));
            // @formatter:on

            // Grouping context should have matched against parent_x. Only parent_1 found in commonality.
            expect(lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_1", "FREDO"));
            expect(lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_0", "SANTINO"));

            assertResult();
        }

        @Test
        public void testPartialMatchWithIndexOfOne() {
            // @formatter:off
            givenArg(Lists.newArrayList(
                            lcNoDiacriticsTuple("NAME.grandparent_0.parent_0.child_1","FREDO"),
                            lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_1","FREDO")));
            givenArg(normalizeLcNoDiacriticsFilter("fredo"));
            givenArg(lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_0","SANTINO"));
            givenArg(normalizeLcNoDiacriticsFilter("SANTINO"));
            givenArg(1);
            // @formatter:on

            // Grouping context should have matched against grandparent_0.
            expect(lcNoDiacriticsTuple("NAME.grandparent_0.parent_0.child_1", "FREDO"));
            expect(lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_1", "FREDO"));
            expect(lcNoDiacriticsTuple("NAME.grandparent_0.parent_1.child_0", "SANTINO"));

            assertResult();
        }

        private void expect(ValueTuple tuple) {
            expected.add(tuple);
        }

        @Override
        protected void assertResult() {
            @SuppressWarnings("unchecked")
            Collection<ValueTuple> valueTuples = (Collection<ValueTuple>) GroupingRequiredFilterFunctions.matchesInGroupLeft(args.toArray(new Object[0]));
            Assertions.assertThat(valueTuples).containsExactlyInAnyOrderElementsOf(expected);
        }
    }
}
