package datawave.query.jexl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.data.type.Type;
import datawave.query.attributes.Document;

public class HitListArithmetic extends DatawaveArithmetic implements StatefulArithmetic {
    
    private static final Logger log = Logger.getLogger(HitListArithmetic.class);
    
    private static final String LESS_THAN = "<", GREATER_THAN = ">", LESS_THAN_OR_EQUAL = "<=", GREATER_THAN_OR_EQUAL = ">=";
    
    private Set<ValueTuple> hitSet = new HashSet<>();
    
    // if exhaustiveHits is true, then all matches are added to a hit set when iterating over a set of values instead of
    // only the first one that matches/equals/etc... This would be more expensive of course but is required for some
    // query logics such as the ancestor query logic.
    private boolean exhaustiveHits = false;
    
    /**
     * Default to being lenient so we don't have to add "null" for every field in the query that doesn't exist in the document
     */
    public HitListArithmetic() {
        super(false);
    }
    
    public HitListArithmetic(boolean exhaustiveHits) {
        this();
        setExhaustiveHits(exhaustiveHits);
    }
    
    /**
     * We need to clone this arithmetic, but we do NOT want to include the stateful elements such as hitSet. This is implemented for the StatefulArithmetic
     * interface.
     */
    @Override
    public HitListArithmetic clone() {
        return new HitListArithmetic(this.exhaustiveHits);
    }
    
    public void setExhaustiveHits(boolean value) {
        this.exhaustiveHits = value;
    }
    
    public boolean isExhaustiveHits() {
        return this.exhaustiveHits;
    }
    
    /**
     * This method differs from the parent in that we are not calling String.matches() because it does not match on a newline. Instead we are handling this
     * case.
     *
     * @param value
     *            first value
     * @param container
     *            second value
     * @return test result.
     */
    @SuppressWarnings("unchecked")
    @Override
    public Boolean contains(final Object container, final Object value) {
        if (value == null && container == null) {
            // if both are null L == R
            return true;
        }
        if (value == null || container == null) {
            // we know both aren't null, therefore L != R
            return false;
        }
        
        Set<Object> elements;
        
        // for every element in left, check if one matches the right pattern
        if (value instanceof Set) {
            elements = (Set<Object>) value;
        } else {
            elements = Collections.singleton(value);
        }
        
        Set<Pattern> patterns;
        if (container instanceof Pattern) {
            patterns = Collections.singleton((Pattern) container);
        } else if (container instanceof Set) {
            patterns = new HashSet<>();
            for (Object r : (Set<Object>) container) {
                if (r instanceof Pattern) {
                    patterns.add((Pattern) r);
                } else {
                    patterns.add(JexlPatternCache.getPattern(r.toString()));
                }
            }
        } else {
            patterns = Collections.singleton(JexlPatternCache.getPattern(container.toString()));
        }
        
        boolean matches = false;
        for (final Object o : elements) {
            // normalize the element
            Object normalizedO = ValueTuple.getNormalizedValue(o);
            Object unnormalizedO = ValueTuple.getValue(o);
            
            for (Pattern p : patterns) {
                if (p.matcher(normalizedO.toString()).matches() || p.matcher(unnormalizedO.toString()).matches()) {
                    this.hitSet.add(ValueTuple.toValueTuple(o));
                    if (!exhaustiveHits) {
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
        }
        
        return matches;
    }
    
    /**
     * This method differs from the parent class in that we are going to try and do a better job of coercing the types. As a last resort we will do a string
     * comparison and try not to throw a NumberFormatException. The JexlArithmetic class performs coercion to a particular type if either the left or the right
     * match a known type. We will look at the type of the right operator and try to make the left of the same type.
     */
    @SuppressWarnings({"unchecked"})
    @Override
    public boolean equals(final Object left, final Object right) {
        // super class takes care of this: left = fixLeft(left, right);
        // When one variable is a Set, treat the equality as #contains
        if (left instanceof Set && !(right instanceof Set)) {
            Set<Object> set = (Set<Object>) left;
            Object normalizedRight = ValueTuple.getNormalizedValue(right);
            boolean matches = false;
            
            for (final Object o : set) {
                // take advantage of numeric conversions
                // normalize the right side value
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                if (super.equals(normalizedO, normalizedRight)) {
                    this.addTheCorrectHitSetValue(hitSet, o, right);
                    if (log.isTraceEnabled()) {
                        log.trace("hitSet now " + hitSet);
                    }
                    if (!exhaustiveHits) {
                        log.trace("equals 1 returning true");
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            return matches;
            
        } else if (!(left instanceof Set) && right instanceof Set) {
            // if multiple possible right hand values, then true if any intersection
            Set<Object> set = (Set<Object>) right;
            
            boolean matches = false;
            Object normalizedLeft = ValueTuple.getNormalizedValue(left);
            for (final Object o : set) {
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                if (equals(normalizedLeft, normalizedO)) {
                    this.addTheCorrectHitSetValue(hitSet, left, o);
                    if (!exhaustiveHits) {
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            return matches;
            
        } else if (left instanceof Set && right instanceof Set) {
            // both are sets
            Set<Object> rightSet = (Set<Object>) right;
            Set<Object> leftSet = (Set<Object>) left;
            for (final Object leftO : leftSet) {
                Object normalizedLeftO = ValueTuple.getNormalizedValue(leftO);
                for (final Object rightO : rightSet) {
                    Object normalizedRightO = ValueTuple.getNormalizedValue(rightO);
                    if (equals(normalizedLeftO, normalizedRightO)) {
                        this.addTheCorrectHitSetValue(hitSet, leftO, rightO);
                        if (log.isTraceEnabled()) {
                            log.trace("hitSet now " + hitSet);
                        }
                        return true;
                    }
                }
            }
            return false;
            
        }
        Object normalizedLeft = ValueTuple.getNormalizedValue(left);
        Object normalizedRight = ValueTuple.getNormalizedValue(right);
        boolean superStatus = super.equals(normalizedLeft, normalizedRight);
        if (superStatus) {
            this.addTheCorrectHitSetValue(hitSet, left, right);
        }
        return superStatus;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean lessThan(final Object left, final Object right) {
        // super class takes care of this: left = fixLeft(left, right);
        // When one variable is a Set, check for existence for one value that satisfies the lessThan operator
        if (left instanceof Set && !(right instanceof Set)) {
            Set<Object> set = (Set<Object>) left;
            boolean matches = false;
            for (final Object o : set) {
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                Object normalizedRight = ValueTuple.getNormalizedValue(right);
                if (compare(normalizedO, normalizedRight, LESS_THAN) < 0) {
                    this.addTheCorrectHitSetValue(hitSet, o, right);
                    if (!exhaustiveHits) {
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            
            return matches;
        } else if (right instanceof Set) {
            Set<Object> set = (Set<Object>) right;
            
            boolean matches = false;
            for (final Object o : set) {
                Object normalizedLeft = ValueTuple.getNormalizedValue(left);
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                if (lessThan(normalizedLeft, normalizedO)) {
                    this.addTheCorrectHitSetValue(hitSet, left, o);
                    
                    if (!exhaustiveHits) {
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            return matches;
        }
        
        Object normalizedLeft = ValueTuple.getNormalizedValue(left);
        Object normalizedRight = ValueTuple.getNormalizedValue(right);
        boolean superStatus = super.lessThan(normalizedLeft, normalizedRight);
        if (superStatus) {
            this.addTheCorrectHitSetValue(hitSet, left, right);
        }
        return superStatus;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean lessThanOrEqual(final Object left, final Object right) {
        // super class takes care of this: left = fixLeft(left, right);
        // When one variable is a Set, check for existence for one value that satisfies the lessThan operator
        if (left instanceof Set && !(right instanceof Set)) {
            Set<Object> set = (Set<Object>) left;
            
            boolean matches = false;
            for (final Object o : set) {
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                Object normalizedRight = ValueTuple.getNormalizedValue(right);
                if (compare(normalizedO, normalizedRight, LESS_THAN_OR_EQUAL) <= 0) {
                    this.addTheCorrectHitSetValue(hitSet, o, right);
                    if (log.isTraceEnabled()) {
                        log.trace("hitSet now " + hitSet);
                    }
                    if (!exhaustiveHits) {
                        log.trace("lessThanOrEqual 1 returning true");
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            
            if (log.isTraceEnabled()) {
                log.trace("lessThanOrEqual 1 returning " + Boolean.toString(matches).toUpperCase());
            }
            return matches;
        } else if (right instanceof Set) {
            Set<Object> set = (Set<Object>) right;
            
            boolean matches = false;
            for (final Object o : set) {
                Object normalizedLeft = ValueTuple.getNormalizedValue(left);
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                if (lessThanOrEqual(normalizedLeft, normalizedO)) {
                    this.addTheCorrectHitSetValue(hitSet, left, o);
                    if (log.isTraceEnabled()) {
                        log.trace("hitSet now " + hitSet);
                    }
                    if (!exhaustiveHits) {
                        log.trace("lessThanOrEqual 2 returning true");
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            
            if (log.isTraceEnabled()) {
                log.trace("lessThanOrEqual 2 returning " + Boolean.toString(matches).toUpperCase());
            }
            return matches;
        }
        
        Object normalizedLeft = ValueTuple.getNormalizedValue(left);
        Object normalizedRight = ValueTuple.getNormalizedValue(right);
        boolean superStatus = super.lessThanOrEqual(normalizedLeft, normalizedRight);
        if (superStatus) {
            this.addTheCorrectHitSetValue(hitSet, left, right);
            if (log.isTraceEnabled()) {
                log.trace("hitSet now " + hitSet);
            }
        }
        return superStatus;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean greaterThan(final Object left, final Object right) {
        // super class takes care of this: left = fixLeft(left, right);
        // When one variable is a Set, check for existence for one value that satisfies the greaterThan operator
        if (left instanceof Set && !(right instanceof Set)) {
            Set<Object> set = (Set<Object>) left;
            
            boolean matches = false;
            for (final Object o : set) {
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                Object normalizedRight = ValueTuple.getNormalizedValue(right);
                if (compare(normalizedO, normalizedRight, GREATER_THAN) > 0) {
                    this.addTheCorrectHitSetValue(hitSet, o, right);
                    if (log.isTraceEnabled()) {
                        log.trace("hitSet now " + hitSet);
                    }
                    if (!exhaustiveHits) {
                        log.trace("greaterThan 1 returning true");
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            
            if (log.isTraceEnabled()) {
                log.trace("greaterThan 1 returning " + Boolean.toString(matches).toUpperCase());
            }
            return matches;
        } else if (right instanceof Set) {
            Set<Object> set = (Set<Object>) right;
            
            boolean matches = false;
            for (final Object o : set) {
                Object normalizedLeft = ValueTuple.getNormalizedValue(left);
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                if (greaterThan(normalizedLeft, normalizedO)) {
                    this.addTheCorrectHitSetValue(hitSet, left, o);
                    if (log.isTraceEnabled()) {
                        log.trace("hitSet now " + hitSet);
                    }
                    if (!exhaustiveHits) {
                        log.trace("greaterThan 2 returning true");
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            
            if (log.isTraceEnabled()) {
                log.trace("greaterThan 2 returning " + Boolean.toString(matches).toUpperCase());
            }
            return matches;
        }
        
        Object normalizedLeft = ValueTuple.getNormalizedValue(left);
        Object normalizedRight = ValueTuple.getNormalizedValue(right);
        boolean superStatus = super.greaterThan(normalizedLeft, normalizedRight);
        if (superStatus) {
            this.addTheCorrectHitSetValue(hitSet, left, right);
            if (log.isTraceEnabled()) {
                log.trace("hitSet now " + hitSet);
            }
        }
        return superStatus;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean greaterThanOrEqual(final Object left, final Object right) {
        // super class takes care of this: left = fixLeft(left, right);
        // When one variable is a Set, check for existence for one value that satisfies the greaterThan operator
        if (left instanceof Set && !(right instanceof Set)) {
            Set<Object> set = (Set<Object>) left;
            
            boolean matches = false;
            for (final Object o : set) {
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                Object normalizedRight = ValueTuple.getNormalizedValue(right);
                if (compare(normalizedO, normalizedRight, GREATER_THAN_OR_EQUAL) >= 0) {
                    this.addTheCorrectHitSetValue(hitSet, o, right);
                    if (log.isTraceEnabled()) {
                        log.trace("hitSet now " + hitSet);
                    }
                    if (!exhaustiveHits) {
                        log.trace("greaterThanOrEqual 1 returning true");
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            
            if (log.isTraceEnabled()) {
                log.trace("greaterThanOrEqual 1 returning " + Boolean.toString(matches).toUpperCase());
            }
            return matches;
        } else if (right instanceof Set) {
            Set<Object> set = (Set<Object>) right;
            
            boolean matches = false;
            for (final Object o : set) {
                Object normalizedLeft = ValueTuple.getNormalizedValue(left);
                Object normalizedO = ValueTuple.getNormalizedValue(o);
                if (greaterThanOrEqual(normalizedLeft, normalizedO)) {
                    this.addTheCorrectHitSetValue(hitSet, left, o);
                    if (log.isTraceEnabled()) {
                        log.trace("hitSet now " + hitSet);
                    }
                    if (!exhaustiveHits) {
                        log.trace("greaterThanOrEqual 2 returning true");
                        return true;
                    } else {
                        matches = true;
                    }
                }
            }
            
            if (log.isTraceEnabled()) {
                log.trace("greaterThanOrEqual 2 returning " + Boolean.toString(matches).toUpperCase());
            }
            return matches;
        }
        
        Object normalizedLeft = ValueTuple.getNormalizedValue(left);
        Object normalizedRight = ValueTuple.getNormalizedValue(right);
        boolean superStatus = super.greaterThanOrEqual(normalizedLeft, normalizedRight);
        if (superStatus) {
            this.addTheCorrectHitSetValue(hitSet, left, right);
            if (log.isTraceEnabled()) {
                log.trace("hitSet now " + hitSet);
            }
        }
        return superStatus;
    }
    
    @Override
    public long toLong(Object val) {
        if (val == null) {
            controlNullOperand();
            return 0L;
        } else if (val instanceof Double) {
            if (!Double.isNaN((Double) val)) {
                return 0;
            } else {
                return ((Double) val).longValue();
            }
        } else if (val instanceof Number) {
            return ((Number) val).longValue();
        } else if (val instanceof String) {
            if ("".equals(val)) {
                return 0;
            } else {
                // val could actually have a mantissa (e.g. "65.0")
                // that would throw a NumberFormatException
                try {
                    return Long.parseLong((String) val);
                } catch (NumberFormatException e) {
                    Double d = Double.parseDouble((String) val);
                    return d.longValue();
                }
            }
        } else if (val instanceof Boolean) {
            return (Boolean) val ? 1L : 0L;
        } else if (val instanceof Character) {
            return (Character) val;
        }
        
        throw new ArithmeticException("Long coercion: " + val.getClass().getName() + ":(" + val + ")");
    }
    
    /**
     * Convert the left hand object if required to the same numberic class as the right hand side.
     * 
     * @param left
     *            the left
     * @param right
     *            the right
     * @return the fixed left hand object
     */
    protected Object fixLeft(Object left, Object right) {
        
        if (null == left || null == right) {
            return left;
        }
        
        Class<? extends Number> rightNumberClass = getNumberClass(right, false);
        boolean rightIsNumber = (rightNumberClass != null);
        
        // if the right is a Number (sans converting String objects)
        if (rightIsNumber) {
            // then convert the left to a number as well
            left = convertToNumber(left, rightNumberClass);
        }
        
        return left;
    }
    
    public Set<ValueTuple> getHitTuples() {
        return hitSet;
    }
    
    public void clear() {
        hitSet.clear();
    }
    
    public void add(ValueTuple hit) {
        hitSet.add(hit);
    }
    
    public Set<String> getHitSet() {
        Set<String> hits = new HashSet<>();
        for (ValueTuple value : hitSet) {
            hits.add(value.getFieldName() + ':' + value.getValue());
        }
        return hits;
    }
    
    private void addTheCorrectHitSetValue(Set<ValueTuple> hitSet, Object left, Object right) {
        // the hitSet should get the value from the one that is a ValueTuple, not the one that is the String
        if (left instanceof ValueTuple) {
            hitSet.add((ValueTuple) left);
        } else if (right instanceof ValueTuple) {
            hitSet.add((ValueTuple) right);
        } else if (log.isTraceEnabled()) {
            log.trace("neither left nor right can be added to hit set left:" + (left != null ? left.getClass() : null) + ", right:"
                            + (right != null ? right.getClass() : null));
        }
    }
    
    public static ColumnVisibility getColumnVisibilityForHit(Document document, String hitTerm) {
        Attribute attr = getAttributeForHit(document, hitTerm);
        if (attr != null) {
            return attr.getColumnVisibility();
        }
        return null;
    }
    
    public static Attribute getAttributeForHit(Document document, String hitTerm) {
        // get the attribute for the record with this hit
        // split the term:
        int idx = hitTerm.indexOf(':');
        if (idx == -1)
            return null; // hitTerm from a composite function, not in Document
        String hitName = hitTerm.substring(0, idx);
        String hitValue = hitTerm.substring(idx + 1);
        Attribute<?> documentAttribute = document.get(hitName);
        if (documentAttribute == null) {
            log.warn("documentAttribute for hitTerm:" + hitTerm + " is null in document:" + document);
        } else if (documentAttribute instanceof Attributes) {
            Attributes documentAttributes = (Attributes) documentAttribute;
            for (Attribute<?> documentAttr : documentAttributes.getAttributes()) {
                if (documentAttr instanceof TypeAttribute) {
                    TypeAttribute<?> typeAttribute = (TypeAttribute) documentAttr;
                    Type<?> type = typeAttribute.getType();
                    Collection<String> expansions = Sets.newHashSet(type.getNormalizedValue(), type.getDelegate().toString());
                    for (String expansion : expansions) {
                        if (expansion.equals(hitValue)) {
                            return documentAttr;
                        }
                    }
                } else if (hitValue.equals(documentAttr.getData())) {
                    return documentAttr;
                }
            }
        } else {
            if (documentAttribute instanceof TypeAttribute) {
                TypeAttribute<?> typeAttribute = (TypeAttribute<?>) documentAttribute;
                Type<?> type = typeAttribute.getType();
                Collection<String> expansions = Sets.newHashSet(type.getNormalizedValue(), type.getDelegate().toString());
                for (String expansion : expansions) {
                    if (expansion.equals(hitValue)) {
                        return documentAttribute;
                    }
                }
            } else if (hitValue.equals(documentAttribute.getData())) {
                return documentAttribute;
            }
        }
        return null; // hitTerm not in Document
    }
    
}
