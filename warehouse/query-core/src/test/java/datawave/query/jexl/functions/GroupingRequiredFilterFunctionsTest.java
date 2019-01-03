package datawave.query.jexl.functions;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

public class GroupingRequiredFilterFunctionsTest {
    
    ValueTuple vt1 = makeValueTuple("FOO.1,BAR,bar");
    ValueTuple vt2 = makeValueTuple("FOO.1,BAR,bar");
    ValueTuple vt3 = makeValueTuple("FOO.1,BAR,bar");
    
    ValueTuple vt4 = makeValueTuple("FOO.1,BAZ,baz");
    
    // different grouping context:
    ValueTuple vt5 = makeValueTuple("FOO.2,BAR,bar");
    
    Collection<?> vt1s = Collections.singleton(vt1);
    Collection<?> vt2s = Collections.singleton(vt2);
    Collection<?> vt3s = Collections.singleton(vt3);
    Collection<?> vt4s = Collections.singleton(vt4);
    Collection<?> vt5s = Collections.singleton(vt5);
    
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
        Assert.assertTrue(GroupingRequiredFilterFunctions.atomValuesMatch(vt1, vt2, vt3).size() > 0);
        Assert.assertTrue(GroupingRequiredFilterFunctions.atomValuesMatch(vt1, vt2, vt4).size() == 0);
        Assert.assertTrue(GroupingRequiredFilterFunctions.atomValuesMatch(vt1, vt5, vt3).size() == 0);
        
        Assert.assertTrue(GroupingRequiredFilterFunctions.atomValuesMatch(vt1s, vt2s, vt3s).size() > 0);
        Assert.assertTrue(GroupingRequiredFilterFunctions.atomValuesMatch(vt1s, vt2s, vt4s).size() == 0);
        Assert.assertTrue(GroupingRequiredFilterFunctions.atomValuesMatch(vt1s, vt5s, vt3s).size() == 0);
    }
    
    @Test
    public void testMatchesInGroup() {
        Assert.assertTrue(GroupingRequiredFilterFunctions.matchesInGroup(vt1, "bar", vt4, "baz").size() > 0);
        Assert.assertTrue(GroupingRequiredFilterFunctions.matchesInGroup(vt1s, "bar", vt4s, "baz").size() > 0);
    }
    
    @Test
    public void testGetGroupsForMatchesInGroup() {
        Collection<?> groups = GroupingRequiredFilterFunctions.getGroupsForMatchesInGroup(vt1, "bar", vt4, "baz");
        Assert.assertTrue(groups.size() > 0);
        Assert.assertTrue(groups.contains("1"));
        
        groups = GroupingRequiredFilterFunctions.getGroupsForMatchesInGroup(vt1s, "bar", vt4s, "baz");
        Assert.assertTrue(groups.size() > 0);
        Assert.assertTrue(groups.contains("1"));
    }
    
    @Test
    public void testMatchesInGroupLeft() {
        Collection<?> groups = GroupingRequiredFilterFunctions.matchesInGroupLeft(makeValueTuple("NAME.grandparent_0.parent_0.child_1,FREDO,fredo"), "fredo",
                        makeValueTuple("NAME.grandparent_0.parent_0.child_0,SANTINO,santino"), "santino");
        
        Assert.assertTrue(groups.size() > 0);
        
        groups = GroupingRequiredFilterFunctions.matchesInGroupLeft(makeValueTuple("NAME.grandparent_0.parent_0.child_1,FREDO,fredo"), "fredo",
                        makeValueTuple("NAME.grandparent_0.parent_1.child_0,SANTINO,santino"), "santino", 1);
        
        Assert.assertTrue(groups.size() > 0);
    }
}
