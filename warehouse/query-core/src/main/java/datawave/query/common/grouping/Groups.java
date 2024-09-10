package datawave.query.common.grouping;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a set of groups found during a #GROUP_BY operation.
 */
public class Groups {

    /**
     * A map of distinct grouping values to their groups.
     */
    private final Map<Grouping,Group> groups = new HashMap<>();

    /**
     * Returns the collection of {@link Group} in this {@link Groups}.
     *
     * @return the groups
     */
    public Collection<Group> getGroups() {
        return groups.values();
    }

    public int totalGroups() {
        return groups.size();
    }

    /**
     * Return whether this {@link Groups} contains a {@link Group} for the given grouping values.
     *
     * @param attributes
     *            the grouping values
     * @return true if this {@link Groups} contains a {@link Group} for the given grouping values, or false otherwise
     */
    public boolean containsGroup(Grouping attributes) {
        return groups.containsKey(attributes);
    }

    /**
     * Return the {@link Group} for the given grouping values, or null if there is no match.
     *
     * @param attributes
     *            the grouping values
     * @return the {@link Group}
     */
    public Group getGroup(Grouping attributes) {
        return groups.get(attributes);
    }

    /**
     * Put the given {@link Group} into this {@link Groups}
     *
     * @param group
     *            the group to put
     */
    public void putGroup(Group group) {
        this.groups.put(group.getGrouping(), group);
    }

    /**
     * If this {@link Groups} already contains a {@link Group} with the grouping values of the given group, the given group will be merged into the existing
     * group. Otherwise, the given group will be put into this {@link Groups}.
     *
     * @param group
     *            the group to merge or put
     */
    public void mergeOrPutGroup(Group group) {
        if (containsGroup(group.getGrouping())) {
            Group existing = getGroup(group.getGrouping());
            existing.merge(group);
        } else {
            putGroup(group);
        }
    }

    public void mergeAll(Groups currentGroups) {
        currentGroups.groups.values().forEach(this::mergeOrPutGroup);
    }

    /**
     * Return whether this {@link Groups} contains any groups.
     *
     * @return true if this {@link Groups} does not contain any groups, or false otherwise
     */
    public boolean isEmpty() {
        return this.groups.isEmpty();
    }

    /**
     * Clears all groups in this {@link Groups}.
     */
    public void clear() {
        this.groups.clear();
    }

    @Override
    public String toString() {
        return groups.toString();
    }

    public void aggregateToAllGroups(Collection<Field> fields) {
        for (Group group : groups.values()) {
            group.aggregateAll(fields);
        }
    }
}
