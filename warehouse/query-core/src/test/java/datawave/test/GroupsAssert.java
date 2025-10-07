package datawave.test;

import java.util.Arrays;

import org.assertj.core.api.AbstractAssert;

import datawave.query.common.grouping.Grouping;
import datawave.query.common.grouping.GroupingAttribute;
import datawave.query.common.grouping.Groups;

public class GroupsAssert extends AbstractAssert<GroupsAssert,Groups> {

    public static GroupsAssert assertThat(Groups groups) {
        return new GroupsAssert(groups);
    }

    protected GroupsAssert(Groups groups) {
        super(groups, GroupsAssert.class);
    }

    public GroupsAssert hasTotalGroups(int total) {
        isNotNull();
        if (total != actual.totalGroups()) {
            failWithMessage("Expected %s total groups, but was %s", total, actual.totalGroups());
        }
        return this;
    }

    public GroupAssert assertGroup(GroupingAttribute<?>... keyElements) {
        isNotNull();
        Grouping grouping = new Grouping();
        grouping.addAll(Arrays.asList(keyElements));
        return GroupAssert.assertThat(actual.getGroup(grouping));
    }

    public GroupAssert assertGroup(Grouping grouping) {
        isNotNull();
        return GroupAssert.assertThat(actual.getGroup(grouping));
    }
}
