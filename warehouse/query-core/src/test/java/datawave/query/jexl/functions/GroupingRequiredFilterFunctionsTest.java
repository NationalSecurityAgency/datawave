package datawave.query.jexl.functions;

import com.google.common.collect.Lists;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;

public class GroupingRequiredFilterFunctionsTest {
    
    private ValueTuple makeValueTuple(String csv) {
        String[] tokens = csv.split(",");
        String field = tokens[0];
        Type<String> type = new LcNoDiacriticsType(tokens[1]);
        TypeAttribute<String> typeAttribute = new TypeAttribute<>(type, new Key(), true);
        String normalized = tokens[2];
        return new ValueTuple(field, type, normalized, typeAttribute);
    }
    
    @Test
    public void testAtomValuesMatch() {
        // @formatter:off
        // should be 3 matches for 'bar', one in each arg
        Assertions.assertEquals(3, GroupingRequiredFilterFunctions.atomValuesMatch(
                makeValueTuple("ALPHA.1,BAR,bar"),
                makeValueTuple("BETA.1,BAR,bar"),
                makeValueTuple("GAMMA.1,BAR,bar")).size());
        
        // no matches across all 3 args
        Assertions.assertEquals(0, GroupingRequiredFilterFunctions.atomValuesMatch(
                makeValueTuple("ALPHA.1,BAR,bar"),
                makeValueTuple("BETA.1,BAR,bar"),
                makeValueTuple("GAMMA.1,BAZ,baz")).size());
        
        // no matches because the 2nd arg has matching 'bar' but in a different grouping context
        Assertions.assertEquals(0, GroupingRequiredFilterFunctions.atomValuesMatch(
                makeValueTuple("ALPHA.1,BAR,bar"),
                makeValueTuple("BETA.2,BAR,bar"), // different context
                makeValueTuple("GAMMA.1,BAR,bar")).size());

        Assertions.assertEquals(3, GroupingRequiredFilterFunctions.atomValuesMatch(
                Collections.singleton(makeValueTuple("ALPHA.1,BAR,bar")),
                Collections.singleton(makeValueTuple("BETA.1,BAR,bar")),
                Collections.singleton(makeValueTuple("GAMMA.1,BAR,bar"))).size());

        Assertions.assertEquals(0, GroupingRequiredFilterFunctions.atomValuesMatch(
                Collections.singleton(makeValueTuple("ALPHA.1,BAR,bar")),
                Collections.singleton(makeValueTuple("BETA.1,BAR,bar")),
                Collections.singleton(makeValueTuple("GAMMA.1,BAZ,baz"))).size());

        Assertions.assertEquals(0, GroupingRequiredFilterFunctions.atomValuesMatch(
                Collections.singleton(makeValueTuple("ALPHA.1,BAR,bar")),
                Collections.singleton(makeValueTuple("BETA.2,BAR,bar")), // different grouping context
                Collections.singleton(makeValueTuple("GAMMA.1,BAR,bar"))).size());
        // @formatter:on
    }
    
    @Test
    public void testAtomValuesMatchMult() {
        // @formatter:off
        // there should be 4 matches, 2 for 'bar' and 2 for 'baz'
        Collection<?> matches = GroupingRequiredFilterFunctions.atomValuesMatch(
                Lists.newArrayList(
                        makeValueTuple("ALPHA.1,BAR,bar"),
                        makeValueTuple("ALPHA.2,BAZ,baz")),
                Lists.newArrayList(
                        makeValueTuple("BETA.1,BAR,bar"),
                        makeValueTuple("BETA.2,BAZ,baz"))
        );
        Assertions.assertEquals(4, matches.size());

        // there should be 3 matches, just the ones with value 'bar' and no matches for 'baz' which is not in the 3rd argument
        matches = GroupingRequiredFilterFunctions.atomValuesMatch(
                Lists.newArrayList(
                        makeValueTuple("ALPHA.1,BAR,bar"),
                        makeValueTuple("ALPHA.2,BAZ,baz")),
                Lists.newArrayList(
                        makeValueTuple("BETA.1,BAR,bar"),
                        makeValueTuple("BETA.2,BAZ,baz")),
                makeValueTuple("GAMMA.1,BAR,bar")//field31);
        );
        Assertions.assertEquals(3, matches.size());


        // there should be no matches, because 'biz', the only member in the 3rd argument, does not match anything in the 1st and 2nd args
        matches = GroupingRequiredFilterFunctions.atomValuesMatch(
                Lists.newArrayList(
                        makeValueTuple("ALPHA.1,BAR,bar"),
                        makeValueTuple("ALPHA.2,BAZ,baz")),
                Lists.newArrayList(
                        makeValueTuple("BETA.1,BAR,bar"),
                        makeValueTuple("BETA.2,BAZ,baz")),
                makeValueTuple("GAMMA.1,BIZ,biz")//field41
        );
        Assertions.assertEquals(0, matches.size());

        // there should be 6 matches, 3 for 'bar', and 3 for 'baz'
        matches = GroupingRequiredFilterFunctions.atomValuesMatch(
                Lists.newArrayList(
                        makeValueTuple("ALPHA.1,BAR,bar"),
                        makeValueTuple("ALPHA.2,BAZ,baz")),
                Lists.newArrayList(
                        makeValueTuple("BETA.1,BAR,bar"),
                        makeValueTuple("BETA.2,BAZ,baz")),
                Lists.newArrayList(
                        makeValueTuple("GAMMA.1,BAR,bar"),
                        makeValueTuple("GAMMA.2,BAZ,baz"))
        );
        Assertions.assertEquals(6, matches.size());
        // @formatter:on
    }
    
    @Test
    public void testMatchesInGroup() {
        // @formatter:off
        Assertions.assertEquals(2, GroupingRequiredFilterFunctions.matchesInGroup(
                makeValueTuple("ALPHA.1,BAR,bar"), "bar",
                makeValueTuple("GAMMA.1,BAZ,baz"), "baz").size());

        Assertions.assertEquals(2, GroupingRequiredFilterFunctions.matchesInGroup(
                Collections.singleton(makeValueTuple("ALPHA.1,BAR,bar")), "bar",
                Collections.singleton(makeValueTuple("GAMMA.1,BAZ,baz")), "baz").size());
        // @formatter:on
    }
    
    @Test
    public void testGetGroupsForMatchesInGroup() {
        // @formatter:off
        // there is only one match and it is in context group .1
        Collection<?> groups = GroupingRequiredFilterFunctions.getGroupsForMatchesInGroup(
                makeValueTuple("ALPHA.1,BAR,bar"), "bar",
                makeValueTuple("GAMMA.1,BAZ,baz"), "baz");
        Assertions.assertEquals(1, groups.size());
        Assertions.assertTrue(groups.contains("1"));
        
        // there is only one match and it is in context group .1
        groups = GroupingRequiredFilterFunctions.getGroupsForMatchesInGroup(
                Collections.singleton(makeValueTuple("ALPHA.1,BAR,bar")), "bar",
                        Collections.singleton(makeValueTuple("GAMMA.1,BAZ,baz")), "baz");
        Assertions.assertEquals(1, groups.size());
        Assertions.assertTrue(groups.contains("1"));
        
        // there is only one match because, while 'bar' matches in both context group2 .1 and .2 for the 1st argument,
        // 'baz' matches only in context group .1 for the 2nd argument
        groups = GroupingRequiredFilterFunctions.getGroupsForMatchesInGroup(
                        Lists.newArrayList(
                                makeValueTuple("ALPHA.1,BAR,bar"),
                                makeValueTuple("ALPHA.2,BAR,bar")), "bar",
                        Collections.singleton(
                                makeValueTuple("GAMMA.1,BAZ,baz")), "baz");
        Assertions.assertEquals(1, groups.size());
        Assertions.assertTrue(groups.contains("1"));
        // @formatter:on
    }
    
    @Test
    public void testMatchesInGroupLeft() {
        // @formatter:off
        Collection<?> groups = GroupingRequiredFilterFunctions.matchesInGroupLeft(
                makeValueTuple("NAME.grandparent_0.parent_0.child_1,FREDO,fredo"), "fredo",
                        makeValueTuple("NAME.grandparent_0.parent_0.child_0,SANTINO,santino"), "santino");

        Assertions.assertEquals(2, groups.size());
        
        groups = GroupingRequiredFilterFunctions.matchesInGroupLeft(
                makeValueTuple("NAME.grandparent_0.parent_0.child_1,FREDO,fredo"), "fredo",
                        makeValueTuple("NAME.grandparent_0.parent_1.child_0,SANTINO,santino"), "santino", 1);

        Assertions.assertEquals(2, groups.size());
        
        // returns 2 matches because the matching groups for 'fredo' in the tuples in the 1st arg is
        // NAME.grandparent_0.parent_0 and NAME.grandparent_0.parent_1
        // but the matching group for 'santino' in the tuple of the 2nd arg is
        // NAME.grandparent_0.parent_1
        groups = GroupingRequiredFilterFunctions.matchesInGroupLeft(Lists.newArrayList(
                makeValueTuple("NAME.grandparent_0.parent_0.child_1,FREDO,fredo"),
                makeValueTuple("NAME.grandparent_0.parent_1.child_1,FREDO,fredo")), "fredo",
                makeValueTuple("NAME.grandparent_0.parent_1.child_0,SANTINO,santino"), "santino", 0);

        Assertions.assertEquals(2, groups.size());
        
        // returns 3 matches because the matching group is 'NAME.grandparent_0' and 'fredo' matches in 2 tuples of the 1st arg
        // 'santino' matches in the tuple of the 2nd arg
        groups = GroupingRequiredFilterFunctions.matchesInGroupLeft(Lists.newArrayList(
                makeValueTuple("NAME.grandparent_0.parent_0.child_1,FREDO,fredo"),
                makeValueTuple("NAME.grandparent_0.parent_1.child_1,FREDO,fredo")), "fredo",
                makeValueTuple("NAME.grandparent_0.parent_1.child_0,SANTINO,santino"), "santino", 1);

        Assertions.assertEquals(3, groups.size());
        // @formatter:on
    }
}
