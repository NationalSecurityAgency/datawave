package datawave.query.jexl.visitors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockDateIndexHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.test.JexlNodeAssert;

public class SetMembershipVisitorTest {

    private static ShardQueryConfiguration config;
    private final Set<String> fields = new HashSet<>();
    private JexlNode query;

    private static Set<String> indexOnly;
    private static MockMetadataHelper helper;
    private static MockDateIndexHelper helper2;

    @BeforeClass
    public static void initializeMetadata() {
        indexOnly = Sets.newHashSet("INDEX_ONLY", "ONLY_INDEXED", "BODY");
        helper = new MockMetadataHelper();
        helper.setIndexedFields(indexOnly);
        helper.setIndexOnlyFields(indexOnly);
        helper.addTermFrequencyFields(Collections.singleton("BODY"));

        helper2 = new MockDateIndexHelper();
        config = new ShardQueryConfiguration();
        config.setDatatypeFilter(Collections.singleton("test"));
    }

    @After
    public void tearDown() throws Exception {
        query = null;
        fields.clear();
    }

    private void assertMembers(String query, Set<String> expectedMembers) throws Exception {
        Assert.assertEquals(expectedMembers, SetMembershipVisitor.getMembers(indexOnly, JexlASTHelper.parseJexlQuery(query)));
    }

    private void assertContains(String query) throws Exception {
        Assert.assertTrue(SetMembershipVisitor.contains(indexOnly, JexlASTHelper.parseJexlQuery(query)));
    }

    @Test
    public void simpleTestMembers() throws Exception {
        assertMembers("INDEX_ONLY == 'foobar'", Sets.newHashSet("INDEX_ONLY"));
    }

    @Test
    public void rangeOperandTestMembers() throws Exception {
        assertMembers("BAR == 'foo' && INDEX_ONLY >= 1", Sets.newHashSet("INDEX_ONLY"));
        assertMembers("BAR == 'foo' && INDEX_ONLY > 1", Sets.newHashSet("INDEX_ONLY"));
        assertMembers("BAR == 'foo' && INDEX_ONLY <= 1", Sets.newHashSet("INDEX_ONLY"));
        assertMembers("BAR == 'foo' && INDEX_ONLY < 1", Sets.newHashSet("INDEX_ONLY"));
    }

    @Test
    public void phraseTestMembers() throws Exception {
        JexlNode node = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2,
                        JexlASTHelper.parseJexlQuery("BAR == 'foo' && content:phrase(termOffsetMap, 'foo', 'bar')"));
        Assert.assertEquals(Sets.newHashSet("BODY"), SetMembershipVisitor.getMembers(indexOnly, node));
    }

    @Test
    public void simpleTestContains() throws Exception {
        assertContains("INDEX_ONLY == 'foobar'");
    }

    @Test
    public void andTestContains() throws Exception {
        assertContains("BAR == 'foo' && INDEX_ONLY == 'foobar'");
        assertContains("INDEX_ONLY == 'foobar' && BAR == 'foo'");
        assertContains("INDEX_ONLY == 'foobar' && ONLY_INDEXED == 'bar'");
    }

    @Test
    public void orTestContains() throws Exception {
        assertContains("BAR == 'foo' || INDEX_ONLY == 'foobar'");
        assertContains("INDEX_ONLY == 'foobar' || BAR == 'foo'");
        assertContains("INDEX_ONLY == 'foobar' || ONLY_INDEXED == 'bar'");
    }

    @Test
    public void functionTestContains() throws Exception {
        assertContains("BAR == 'foo' && filter:isNull(INDEX_ONLY)");
        assertContains("(filter:isNull(INDEX_ONLY) && FOO  == 'bar') || BAR == 'foo'");
        assertContains("(filter:isNull(INDEX_ONLY) && FOO  == 'bar') || ONLY_INDEXED == 'bar'");
    }

    @Test
    public void andTestMembers() throws Exception {
        assertContains("BAR == 'foo' && INDEX_ONLY == 'foobar'");
        assertContains("INDEX_ONLY == 'foobar' && BAR == 'foo'");
        assertContains("INDEX_ONLY == 'foobar' && ONLY_INDEXED == 'bar'");
    }

    @Test
    public void orTestMembers() throws Exception {
        assertContains("BAR == 'foo' || INDEX_ONLY == 'foobar'");
        assertContains("INDEX_ONLY == 'foobar' || BAR == 'foo'");
        assertContains("INDEX_ONLY == 'foobar' || ONLY_INDEXED == 'bar'");
    }

    @Test
    public void orAndTestMembers() throws Exception {
        assertMembers("BAR == 'foo' || (INDEX_ONLY == 'foobar' && FOO == 'bar')", Sets.newHashSet("INDEX_ONLY"));
        assertMembers("(INDEX_ONLY == 'foobar' && FOO == 'bar') || BAR == 'foo'", Sets.newHashSet("INDEX_ONLY"));
        assertMembers("(INDEX_ONLY == 'foobar' && FOO == 'bar') || ONLY_INDEXED == 'bar'", Sets.newHashSet("INDEX_ONLY", "ONLY_INDEXED"));
    }

    @Test
    public void rangeOperandTestContains() throws Exception {
        assertContains("BAR == 'foo' && INDEX_ONLY >= 1");
        assertContains("BAR == 'foo' && INDEX_ONLY > 1");
        assertContains("BAR == 'foo' && INDEX_ONLY <= 1");
        assertContains("BAR == 'foo' && INDEX_ONLY < 1");
    }

    @Test
    public void phraseTestContains() throws Exception {
        JexlNode node = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2,
                        JexlASTHelper.parseJexlQuery("BAR == 'foo' && content:phrase(termOffsetMap, 'foo', 'bar')"));
        Assert.assertTrue(SetMembershipVisitor.contains(indexOnly, node));
    }

    /**
     * Verify that {@link SetMembershipVisitor#contains(Set, JexlNode)} returns false for a query that does not contain any of the target fields.
     */
    @Test
    public void testContainsForQueryWithoutMatchingFields() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");
        givenQuery("(FOO == 'bar') && (BAT == 'aaa')");

        assertContains(false);
    }

    /**
     * Verify that {@link SetMembershipVisitor#contains(Set, JexlNode)} returns true for a query that contain at least one of the target fields.
     */
    @Test
    public void testContainsForQueryWithMatchingFields() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");

        // Test EQ identifiers.
        givenQuery("(CITY == 'bar')");
        assertContains(true);

        // Test NE identifiers.
        givenQuery("(CITY != 'bar')");
        assertContains(true);

        // Test LT identifiers.
        givenQuery("(CITY < 'bar')");
        assertContains(true);

        // Test GT identifiers.
        givenQuery("(CITY > 'bar')");
        assertContains(true);

        // Test LE identifiers.
        givenQuery("(CITY <= 'bar')");
        assertContains(true);

        // Test GE identifiers.
        givenQuery("(CITY >= 'bar')");
        assertContains(true);

        // Test ER identifiers.
        givenQuery("(CITY =~ 'bar')");
        assertContains(true);

        // Test NR identifiers.
        givenQuery("(CITY !~ 'bar')");
        assertContains(true);

        // Test function identifiers.
        givenQuery("filter:isNull(CITY)");
        assertContains(true);
    }

    /**
     * Verify that {@link SetMembershipVisitor#getMembers(Set, JexlNode)} returns an empty set for a query that does not contain any of the target fields.
     */
    @Test
    public void testGetMembersWithoutMatchingFields() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");
        givenQuery("(FOO == 'bar') && (BAT == 'aaa')");

        assertMembership();
    }

    /**
     * Verify that {@link SetMembershipVisitor#getMembers(Set, JexlNode)} returns a set that contains all fields in the query that were members of the target
     * fields.
     */
    @Test
    public void testGetMembersWithMatchingFields() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");
        givenQuery("(CITY == 'bar') && (NAME == 'aaa' || BAR == 'bbb')");

        assertMembership("CITY", "NAME");
    }

    /**
     * Verify that {@link SetMembershipVisitor#getMembers(Set, JexlNode)} returns an empty set for a query that does not contain any of the target fields.
     */
    @Test
    public void testGetMembersWithIndexOnlyFieldTaggingDisabledWithoutMatches() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");
        givenQuery("(FOO == 'bar') && (BAT == 'aaa')");

        assertMembership();
    }

    /**
     * Verify that {@link SetMembershipVisitor#getMembers(Set, JexlNode)} returns a set that contains all fields in the query that were members of the target
     * fields.
     */
    @Test
    public void testGetMembersWithIndexOnlyFieldTaggingDisabledWithMatches() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");
        givenQuery("(CITY == 'bar') && (NAME == 'aaa' || BAR == 'bbb')");

        assertMembership("CITY", "NAME");
    }

    /**
     * Verify that given a matching index-only field in a filter function with index-only field tagging disabled, that a set is returned containing the field
     * and that the original query is not modified.
     */
    @Test
    public void testIndexOnlyFieldInFilterFunctionsWithTaggingDisabled() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");
        givenQuery("filter:includeRegex(NAME, 'aaa|bbb')");

        assertMembership("NAME");
    }

    /**
     * Given that index fields should be tagged, and a query that does not contain any matches, verify that
     * {@link SetMembershipVisitor#getMembers(Set, JexlNode)} returns an empty set and that the original query is not modified.
     */
    @Test
    public void testGetMembersWithIndexOnlyFieldTaggingEnabledWithoutMatches() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");
        givenQuery("(FOO == 'bar') && (BAT == 'aaa')");

        assertMembershipWithTagging();
        assertQuery("(FOO == 'bar') && (BAT == 'aaa')");
    }

    /**
     * Given that index fields should be tagged, and a query that contains matches, but none in a filter function, verify that
     * {@link SetMembershipVisitor#getMembers(Set, JexlNode)} returns a set of the matching fields, and that the original query was not modified.
     */
    @Test
    public void testMatchingIndexOnlyFieldsNotInFilterFunctions() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");
        givenQuery("(CITY == 'bar') && (NAME == 'aaa' || BAR == 'bbb')");

        assertMembershipWithTagging("CITY", "NAME");
        assertQuery("(CITY == 'bar') && (NAME == 'aaa' || BAR == 'bbb')");
    }

    /**
     * Given that index fields should be tagged, and a query that contains matches in a non-filter function, verify that
     * {@link SetMembershipVisitor#getMembers(Set, JexlNode)} returns a set of the matching fields, and that the original query was not modified.
     */
    @Test
    public void testTaggingIndexOnlyInNonFilterFunctions() throws ParseException {
        givenFields("CITY", "NAME", "COUNTY");
        givenQuery("CITY == 'bar' && f:unique(NAME, BAR)");

        assertMembershipWithTagging("CITY", "NAME");
        assertQuery("CITY == 'bar' && f:unique(NAME, BAR)");
    }

    private void givenQuery(String queryString) throws ParseException {
        query = JexlASTHelper.parseJexlQuery(queryString);
    }

    private void givenFields(String... fields) {
        Collections.addAll(this.fields, fields);
    }

    private void assertContains(boolean expected) {
        assertThat(SetMembershipVisitor.contains(fields, query)).isEqualTo(expected);
    }

    private void assertMembership(String... expected) {
        assertThat(SetMembershipVisitor.getMembers(fields, query)).containsExactly(expected);
    }

    private void assertMembershipWithTagging(String... expected) {
        assertThat(SetMembershipVisitor.getMembers(fields, query)).containsExactly(expected);
    }

    private void assertQuery(String expected) {
        JexlNodeAssert.assertThat(query).hasExactQueryString(expected);
    }
}
