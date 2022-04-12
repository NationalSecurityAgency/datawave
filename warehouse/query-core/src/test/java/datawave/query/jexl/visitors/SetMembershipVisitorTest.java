package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class SetMembershipVisitorTest {
    
    /**
     * Verify that {@link SetMembershipVisitor#contains(Set, ShardQueryConfiguration, JexlNode)} returns false for a query that does not contain any of the
     * target fields.
     */
    @Test
    public void testContainsForQueryWithoutMatchingFields() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("(FOO == 'bar') && (BAT == 'aaa')");
        
        boolean contains = SetMembershipVisitor.contains(fields, config, query);
        assertThat(contains).isFalse();
    }
    
    /**
     * Verify that {@link SetMembershipVisitor#contains(Set, ShardQueryConfiguration, JexlNode)} returns true for a query that contain at least one of the
     * target fields.
     */
    @Test
    public void testContainsForQueryWithMatchingFields() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        
        // Test EQ identifiers.
        JexlNode query = JexlASTHelper.parseJexlQuery("(CITY == 'bar')");
        assertThat(SetMembershipVisitor.contains(fields, config, query)).isTrue();
        
        // Test NE identifiers.
        query = JexlASTHelper.parseJexlQuery("(CITY != 'bar')");
        assertThat(SetMembershipVisitor.contains(fields, config, query)).isTrue();
        
        // Test LT identifiers.
        query = JexlASTHelper.parseJexlQuery("(CITY < 'bar')");
        assertThat(SetMembershipVisitor.contains(fields, config, query)).isTrue();
        
        // Test GT identifiers.
        query = JexlASTHelper.parseJexlQuery("(CITY > 'bar')");
        assertThat(SetMembershipVisitor.contains(fields, config, query)).isTrue();
        
        // Test LE identifiers.
        query = JexlASTHelper.parseJexlQuery("(CITY <= 'bar')");
        assertThat(SetMembershipVisitor.contains(fields, config, query)).isTrue();
        
        // Test GE identifiers.
        query = JexlASTHelper.parseJexlQuery("(CITY >= 'bar')");
        assertThat(SetMembershipVisitor.contains(fields, config, query)).isTrue();
        
        // Test ER identifiers.
        query = JexlASTHelper.parseJexlQuery("(CITY =~ 'bar')");
        assertThat(SetMembershipVisitor.contains(fields, config, query)).isTrue();
        
        // Test NR identifiers.
        query = JexlASTHelper.parseJexlQuery("(CITY !~ 'bar')");
        assertThat(SetMembershipVisitor.contains(fields, config, query)).isTrue();
        
        // Test function identifiers.
        query = JexlASTHelper.parseJexlQuery("filter:isNull(CITY)");
        assertThat(SetMembershipVisitor.contains(fields, config, query)).isTrue();
    }
    
    /**
     * Verify that {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode)} returns an empty set for a query that does not contain any of
     * the target fields.
     */
    @Test
    public void testGetMembersWithoutMatchingFields() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("(FOO == 'bar') && (BAT == 'aaa')");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query);
        assertThat(discoveredFields).isEmpty();
    }
    
    /**
     * Verify that {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode)} returns a set that contains all fields in the query that were
     * members of the target fields.
     */
    @Test
    public void testGetMembersWithMatchingFields() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("(CITY == 'bar') && (NAME == 'aaa' || BAR == 'bbb')");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query);
        assertThat(discoveredFields).containsExactly("CITY", "NAME");
    }
    
    /**
     * Verify that {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} returns an empty set for a query that does not
     * contain any of the target fields.
     */
    @Test
    public void testGetMembersWithIndexOnlyFieldTaggingDisabledWithoutMatches() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("(FOO == 'bar') && (BAT == 'aaa')");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query, false);
        assertThat(discoveredFields).isEmpty();
    }
    
    /**
     * Verify that {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} returns a set that contains all fields in the query
     * that were members of the target fields.
     */
    @Test
    public void testGetMembersWithIndexOnlyFieldTaggingDisabledWithMatches() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("(CITY == 'bar') && (NAME == 'aaa' || BAR == 'bbb')");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query, false);
        assertThat(discoveredFields).containsExactly("CITY", "NAME");
    }
    
    /**
     * Verify that given a matching index-only field in a filter function with index-only field tagging disabled, that a set is returned containing the field
     * and that the original query is not modified.
     */
    @Test
    public void testIndexOnlyFieldInFilterFunctionsWithTaggingDisabled() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("filter:includeRegex(NAME, 'aaa|bbb')");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query, false);
        assertThat(discoveredFields).containsExactly("NAME");
    }
    
    /**
     * Given that index fields should be tagged, and a query that does not contain any matches, verify that
     * {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} returns an empty set and that the original query is not
     * modified.
     */
    @Test
    public void testGetMembersWithIndexOnlyFieldTaggingEnabledWithoutMatches() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("(FOO == 'bar') && (BAT == 'aaa')");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query, true);
        assertThat(discoveredFields).isEmpty();
        JexlNodeAssert.assertThat(query).hasExactQueryString("(FOO == 'bar') && (BAT == 'aaa')");
    }
    
    /**
     * Given that index fields should be tagged, and a query that contains matches, but none in a filter function, verify that
     * {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} returns a set of the matching fields, and that the original
     * query was not modified.
     */
    @Test
    public void testMatchingIndexOnlyFieldsNotInFilterFunctions() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("(CITY == 'bar') && (NAME == 'aaa' || BAR == 'bbb')");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query, true);
        assertThat(discoveredFields).containsExactly("CITY", "NAME");
        JexlNodeAssert.assertThat(query).hasExactQueryString("(CITY == 'bar') && (NAME == 'aaa' || BAR == 'bbb')");
    }
    
    /**
     * Given that index fields should be tagged, and a query that contains matches in a filter function, verify that
     * {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} returns a set of the matching fields, and that the original
     * query has been modified to contain tagged index-only fields.
     */
    @Test
    public void testTaggingIndexOnlyInFilterFunctions() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setLazySetMechanismEnabled(true);
        
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("CITY == 'bar' && filter:includeRegex(NAME, 'aaa|bbb')");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query, true);
        assertThat(discoveredFields).containsExactly("CITY", "NAME");
        JexlNodeAssert.assertThat(query).hasExactQueryString(
                        "CITY == 'bar' && filter:includeRegex(NAME@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION, 'aaa|bbb')");
    }
    
    /**
     * Given that index fields should be tagged, and a query that contains matches in a non-filter function, verify that
     * {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} returns a set of the matching fields, and that the original
     * query was not modified.
     */
    @Test
    public void testTaggingIndexOnlyInNonFilterFunctions() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setLazySetMechanismEnabled(true);
        
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("CITY == 'bar' && f:unique(NAME, BAR)");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query, true);
        assertThat(discoveredFields).containsExactly("CITY", "NAME");
        JexlNodeAssert.assertThat(query).hasExactQueryString("CITY == 'bar' && f:unique(NAME, BAR)");
    }
    
    /**
     * Given that index fields should be tagged, and a query that contains matches in a filter function that have already been tagged, verify that
     * {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} returns a set of the matching fields, and that the original
     * query was not modified.
     */
    @Test
    public void testPreviouslyTaggedIndexOnlyFunctions() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setLazySetMechanismEnabled(true);
        
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("CITY == 'bar' && filter:includeRegex(NAME@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION, 'aaa|bbb')");
        
        Set<String> discoveredFields = SetMembershipVisitor.getMembers(fields, config, query, true);
        assertThat(discoveredFields).containsExactly("CITY", "NAME");
        JexlNodeAssert.assertThat(query).hasExactQueryString(
                        "CITY == 'bar' && filter:includeRegex(NAME@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION, 'aaa|bbb')");
    }
    
    /**
     * Given that index fields should be tagged, and a query that contains matches in a filter function, but given that the LAZY_SET mechanism is disabled,
     * verify that {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} throws an exception.
     */
    @Test
    public void testIndexOnlyFieldTaggingWhenLazySetMechanismIsDisabled() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setLazySetMechanismEnabled(false);
        
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("CITY == 'bar' && filter:includeRegex(NAME, 'aaa|bbb')");
        
        Assertions.assertThatExceptionOfType(DatawaveFatalQueryException.class).isThrownBy(() -> SetMembershipVisitor.getMembers(fields, config, query, true))
                        .withMessage("LAZY_SET mechanism is disabled for index-only fields");
    }
    
    /**
     * Given that index fields should be tagged, and a query that contains already tagged index-only fields in a filter function, but given that the LAZY_SET
     * mechanism is disabled, verify that {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} throws an exception.
     */
    @Test
    public void testPreviouslyTaggedIndexOnlyFieldWhenLazySetMechanismIsDisabled() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setLazySetMechanismEnabled(false);
        
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("CITY == 'bar' && filter:includeRegex(NAME@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION, 'aaa|bbb')");
        
        Assertions.assertThatExceptionOfType(DatawaveFatalQueryException.class).isThrownBy(() -> SetMembershipVisitor.getMembers(fields, config, query, true))
                        .withMessage("LAZY_SET mechanism is disabled for index-only fields");
    }
    
    /**
     * Given that index fields should not be tagged, and a query that contains an already tagged field that is not a match, and that the LAZY_SET mechanism is
     * disabled, verify that {@link SetMembershipVisitor#getMembers(Set, ShardQueryConfiguration, JexlNode, boolean)} throws an exception.
     */
    @Test
    public void testPreviouslyTaggedNonMatchingFieldWhenLazySetMechanismIsDisabled() throws ParseException {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setLazySetMechanismEnabled(false);
        
        Set<String> fields = Sets.newHashSet("CITY", "NAME", "COUNTY");
        JexlNode query = JexlASTHelper.parseJexlQuery("NON_MATCH@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION == 'aaa'");
        
        Assertions.assertThatExceptionOfType(DatawaveFatalQueryException.class).isThrownBy(() -> SetMembershipVisitor.getMembers(fields, config, query, false))
                        .withMessage("LAZY_SET mechanism is disabled for index-only fields");
    }
}
