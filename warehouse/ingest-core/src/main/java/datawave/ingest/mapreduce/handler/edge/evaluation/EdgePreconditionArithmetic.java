package datawave.ingest.mapreduce.handler.edge.evaluation;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.JexlArithmetic;

import datawave.attribute.EventField;
import datawave.attribute.EventFieldValueTuple;

public class EdgePreconditionArithmetic extends JexlArithmetic {

    private Map<String,Set<String>> matchingGroups = new HashMap<>();
    private Map<String,Set<String>> excludedGroups = new HashMap<>();

    private boolean negated = false;

    public EdgePreconditionArithmetic() {
        super(false);
    }

    /*
     * Currently only EQ options are supplied.
     */

    @Override
    public boolean equals(final Object left, final Object right) {
        boolean matches = false;

        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = EventFieldValueTuple.getValue(right);

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = EventFieldValueTuple.getValue(tuple);
                if (super.equals(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }

        } else if (!(left instanceof Collection) && (right instanceof Collection)) {
            Object newLeft = EventFieldValueTuple.getValue(left);

            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = EventFieldValueTuple.getValue(tuple);
                if (super.equals(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }

        } else if ((left instanceof Collection) && (right instanceof Collection)) {

            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object lefttuple = iter.next();
                Iterator iter2 = ((Collection) left).iterator();
                while (iter2.hasNext()) {
                    Object righttuple = iter2.next();
                    Object newLeft = EventFieldValueTuple.getValue(lefttuple);
                    Object newRight = EventFieldValueTuple.getValue(righttuple);
                    if (super.equals(newLeft, newRight)) {
                        addMatchingGroup(righttuple);
                        addMatchingGroup(lefttuple);
                        matches = true;
                    }
                }
            }
        } else {
            Object newLeft = EventFieldValueTuple.getValue(left);
            Object newRight = EventFieldValueTuple.getValue(right);

            if (super.equals(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;

            }
        }

        return matches;
    }

    @Override
    public Boolean contains(Object left, Object right) {
        boolean matches = false;

        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = EventFieldValueTuple.getValue(right);

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = EventFieldValueTuple.getValue(tuple);
                if (super.contains(newRight, newLeft)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }

        } else if (!(left instanceof Collection) && (right instanceof Collection)) {
            throw new IllegalArgumentException("Please ensure regular expression preconditions are of the form 'FIELD =~ regex' and not reversed.");

        } else if ((left instanceof Collection) && (right instanceof Collection)) {

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object lefttuple = iter.next();
                Iterator iter2 = ((Collection) right).iterator();
                while (iter2.hasNext()) {
                    Object righttuple = iter2.next();
                    Object newLeft = EventFieldValueTuple.getValue(lefttuple);
                    Object newRight = EventFieldValueTuple.getValue(righttuple);
                    if (super.contains(newRight, newLeft) || newLeft.toString().contains(newRight.toString())) {
                        addMatchingGroup(righttuple);
                        addMatchingGroup(lefttuple);
                        matches = true;
                    }
                }
            }
        } else {
            Object newLeft = EventFieldValueTuple.getValue(left);
            Object newRight = EventFieldValueTuple.getValue(right);

            if (super.contains(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;

            }
        }
        return matches;
    }

    @Override
    public boolean lessThan(final Object left, final Object right) {
        boolean matches = false;

        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = EventFieldValueTuple.getValue(right);

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.lessThan(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }

        } else if (right instanceof Collection) {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));

            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.lessThan(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
        }

        else {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));

            if (super.lessThan(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;

            }
        }

        return matches;
    }

    @Override
    public boolean lessThanOrEqual(final Object left, final Object right) {
        boolean matches = false;

        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.lessThanOrEqual(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }

        } else if (right instanceof Collection) {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));

            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.lessThanOrEqual(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
        }

        else {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));

            if (super.lessThanOrEqual(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;

            }
        }

        return matches;
    }

    @Override
    public boolean greaterThan(final Object left, final Object right) {
        boolean matches = false;

        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.greaterThan(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }

        } else if (right instanceof Collection) {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));

            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.greaterThan(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
        }

        else {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));

            if (super.greaterThan(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;

            }
        }

        return matches;
    }

    @Override
    public boolean greaterThanOrEqual(final Object left, final Object right) {
        boolean matches = false;

        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.greaterThanOrEqual(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }

        } else if (right instanceof Collection) {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));

            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = Long.parseLong(EventFieldValueTuple.getValue(tuple));
                if (super.greaterThanOrEqual(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                }
            }
        }

        else {
            Object newLeft = Long.parseLong(EventFieldValueTuple.getValue(left));
            Object newRight = Long.parseLong(EventFieldValueTuple.getValue(right));

            if (super.greaterThanOrEqual(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;

            }
        }

        return matches;
    }

    private void addMatchingGroup(Object o) {
        if (o instanceof EventFieldValueTuple) {
            String fieldName = EventFieldValueTuple.getFieldName(o);
            String commonality = EventField.getGroup(fieldName);
            String group = EventField.getSubgroup(fieldName);
            Set<String> groups = matchingGroups.get(commonality);
            if (groups == null) {
                groups = new HashSet<>();
                groups.add(group);
                matchingGroups.put(commonality, groups);
            } else
                groups.add(group);
        }
    }

    private void addExcludedGroup(Object o) {
        if (o instanceof EventFieldValueTuple) {
            String fieldName = EventFieldValueTuple.getFieldName(o);
            String commonality = EventField.getGroup(fieldName);
            String group = EventField.getSubgroup(fieldName);
            Set<String> groups = excludedGroups.get(commonality);
            if (groups == null) {
                groups = new HashSet<>();
                groups.add(group);
                excludedGroups.put(commonality, groups);
            } else
                groups.add(group);
        }
    }

    public Map<String,Set<String>> getMatchingGroups() {
        return matchingGroups;
    }

    public Map<String,Set<String>> getExcludedGroups() {
        return excludedGroups;
    }

    public void clearMatchingGroups() {
        matchingGroups = new HashMap<>();
    }

    public void clearExcludedGroups() {
        excludedGroups = new HashMap<>();
    }

    public void clear() {
        clearMatchingGroups();
        clearExcludedGroups();
    }

    public Object notEquals(Object left, Object right) {
        boolean matches = false;

        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = EventFieldValueTuple.getValue(right);

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = EventFieldValueTuple.getValue(tuple);
                if (!super.equals(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                } else {
                    addExcludedGroup(tuple);
                }
            }

        } else if (!(left instanceof Collection) && (right instanceof Collection)) {
            Object newLeft = EventFieldValueTuple.getValue(left);

            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newRight = EventFieldValueTuple.getValue(tuple);
                if (!super.equals(newLeft, newRight)) {
                    addMatchingGroup(tuple);
                    matches = true;
                } else {
                    addExcludedGroup(tuple);
                }
            }

        } else if ((left instanceof Collection) && (right instanceof Collection)) {

            Iterator iter = ((Collection) right).iterator();
            while (iter.hasNext()) {
                Object lefttuple = iter.next();
                Iterator iter2 = ((Collection) left).iterator();
                while (iter2.hasNext()) {
                    Object righttuple = iter2.next();
                    Object newLeft = EventFieldValueTuple.getValue(lefttuple);
                    Object newRight = EventFieldValueTuple.getValue(righttuple);
                    if (!super.equals(newLeft, newRight)) {
                        addMatchingGroup(righttuple);
                        addMatchingGroup(lefttuple);
                        matches = true;
                    } else {
                        addExcludedGroup(righttuple);
                        addExcludedGroup(lefttuple);
                    }
                }
            }
        } else {
            Object newLeft = EventFieldValueTuple.getValue(left);
            Object newRight = EventFieldValueTuple.getValue(right);

            if (!super.equals(newLeft, newRight)) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;
            } else {
                addExcludedGroup(newLeft);
                addExcludedGroup(newRight);
            }
        }

        return matches;
    }

    public Object notContains(Object left, Object right) {
        boolean matches = false;

        if (left instanceof Collection && !(right instanceof Collection)) {
            Object newRight = EventFieldValueTuple.getValue(right);

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object tuple = iter.next();
                Object newLeft = EventFieldValueTuple.getValue(tuple);
                if (!super.contains(newRight, newLeft)) {
                    addMatchingGroup(tuple);
                    matches = true;
                } else {
                    addExcludedGroup(tuple);
                }
            }

        } else if (!(left instanceof Collection) && (right instanceof Collection)) {

            throw new IllegalArgumentException("Please ensure regular expression preconditions are of the form 'FIELD =~ regex' and not reversed.");

        } else if ((left instanceof Collection) && (right instanceof Collection)) {

            Iterator iter = ((Collection) left).iterator();
            while (iter.hasNext()) {
                Object lefttuple = iter.next();
                Iterator iter2 = ((Collection) right).iterator();
                while (iter2.hasNext()) {
                    Object righttuple = iter2.next();
                    Object newLeft = EventFieldValueTuple.getValue(lefttuple);
                    Object newRight = EventFieldValueTuple.getValue(righttuple);
                    if (!super.contains(newRight, newLeft) && !newLeft.toString().contains(newRight.toString())) {
                        addMatchingGroup(righttuple);
                        addMatchingGroup(lefttuple);
                        matches = true;
                    } else {
                        addExcludedGroup(righttuple);
                        addExcludedGroup(lefttuple);
                    }
                }
            }
        } else {
            Object newLeft = EventFieldValueTuple.getValue(left);
            Object newRight = EventFieldValueTuple.getValue(right);

            if (!super.contains(newRight, newLeft) && !newLeft.toString().contains(newRight.toString())) {
                addMatchingGroup(newLeft);
                addMatchingGroup(newRight);
                matches = true;
            } else {
                addExcludedGroup(newLeft);
                addExcludedGroup(newRight);

            }
        }
        return matches;
    }

}
