package datawave.query.common.grouping;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Groups {
    
    private final Map<Set<GroupingAttribute<?>>,Group> groups = new HashMap<>();
    
    public Set<Set<GroupingAttribute<?>>> getGroupingAttributes() {
        return groups.keySet();
    }
    
    public Collection<Group> getGroups() {
        return groups.values();
    }
    
    public boolean containsGroup(Set<GroupingAttribute<?>> attributes) {
        return groups.containsKey(attributes);
    }
    
    public Group getGroup(Set<GroupingAttribute<?>> attributes) {
        return groups.get(attributes);
    }
    
    public void putGroup(Group group) {
        this.groups.put(group.getAttributes(), group);
    }
    
    public void mergeOrPutGroup(Group group) {
        if (containsGroup(group.getAttributes())) {
            Group existing = getGroup(group.getAttributes());
            existing.merge(group);
        } else {
            putGroup(group);
        }
    }
    
    public boolean isEmpty() {
        return this.groups.isEmpty();
    }
    
    public void clear() {
        this.groups.clear();
    }
    
    @Override
    public String toString() {
        return groups.toString();
    }
}
