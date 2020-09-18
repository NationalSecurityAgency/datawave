package datawave.query.jexl.visitors;

import com.google.common.collect.ImmutableSet;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeBag;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.TypeAttribute;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.predicate.PeekingPredicate;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static datawave.query.Constants.EMPTY_STRING;

/**
 * The EventDataQueryExpressionVisitor traverses the query parse tree and generates a series of ExpressionFilters that will be used to determine if Keys
 * traversed during a scan need to be retained in order to properly evaluate a query. In some cases a given field may have a large number of values, and it is
 * wasteful to load these into memory of they are not necessary for query evaluation.
 */
public class EventDataQueryExpressionVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(EventDataQueryExpressionVisitor.class);
    
    /**
     * ExpressionFilter is used to select those Keys that are required in order to evaluate a query.
     *
     * Each instance of this class corresponds to a single field and may contain a set of literal values, patterns, ranges or a nullValueFlag. These are
     * populated by the EventDataQueryExpressionVisitor as it traverses the query parse tree.
     *
     * When traversing tables at query time, the apply method is called for the keys encountered. If the field value embedded in the key matches the literal,
     * pattern or range, the key is kept indicating that the key is necessary for evaluating that portion of the query.
     *
     * The null value flag is a bit of an unusual case and is generated in cases where a null (or non-null) comparison is performed for a field. It indicates
     * that at least one key for a field must be kept regardless of value. As such, this flag is set to false once its predicate has be satisfied.
     */
    public static class ExpressionFilter implements PeekingPredicate<Key>, Cloneable {
        final AttributeFactory attributeFactory;
        
        /** The field this filter apples to */
        final String fieldName;
        /** fieldValues contains a set of literal values for which we need to keep data in order to satisfy the query */
        final Set<String> fieldValues;
        /** fieldPatterns contains a set of patterns for which we need to keep data in order to satisfy the query */
        final Map<Pattern,Matcher> fieldPatterns;
        /** fieldRanges contains a set of ranges for which we need to keep data in order to satisfy the query */
        final Set<LiteralRange<?>> fieldRanges;
        /** nullValueFlag indicates that we need to capture at least one instance of the field in order to satisfy a null value check in the query */
        boolean nullValueFlag;
        /** nonNullValueSeen indicates that we have seen a nonNullValue. This is used with the nullValueFlag. */
        boolean nonNullValueSeen;
        /** acceptAll indicates that all values should be returned */
        boolean acceptAll;
        
        public ExpressionFilter(AttributeFactory attributeFactory, String fieldName) {
            this.attributeFactory = attributeFactory;
            this.fieldName = fieldName;
            this.fieldValues = new HashSet<>();
            this.fieldPatterns = new HashMap<>();
            this.fieldRanges = new HashSet<>();
            this.nullValueFlag = false;
            this.nonNullValueSeen = false;
            this.acceptAll = false;
        }
        
        public ExpressionFilter(ExpressionFilter other) {
            this(other.attributeFactory, other.fieldName);
            this.fieldValues.addAll(other.fieldValues);
            // making new Matcher objects as they are not thread safe.
            for (Map.Entry<Pattern,Matcher> entry : other.fieldPatterns.entrySet()) {
                this.fieldPatterns.put(entry.getKey(), entry.getKey().matcher(""));
            }
            this.fieldRanges.addAll(other.fieldRanges);
            this.nullValueFlag = other.nullValueFlag;
            this.nonNullValueSeen = other.nonNullValueSeen;
            this.acceptAll = other.acceptAll;
        }
        
        @Override
        public ExpressionFilter clone() {
            return new ExpressionFilter(this);
        }
        
        public void reset() {
            nonNullValueSeen = false;
        }
        
        public String getFieldName() {
            return this.fieldName;
        }
        
        public Set<String> getFieldValues() {
            return ImmutableSet.copyOf(fieldValues);
        }
        
        public void addFieldValue(String value) {
            fieldValues.add(value);
        }
        
        public void setNullValueFlag() {
            nullValueFlag = true;
        }
        
        public void addFieldPattern(String pattern) {
            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher("");
            fieldPatterns.put(p, m);
        }
        
        public void addFieldRange(LiteralRange range) {
            fieldRanges.add(range);
        }
        
        public void acceptAllValues() {
            acceptAll = true;
        }
        
        /**
         *
         * @param key
         *            the key to evaluate, must be parsable by DatawaveKey
         * @return true if the key should be kept in order to evaluare the query, false otherwise
         */
        @Override
        public boolean apply(Key key) {
            return apply(key, true);
        }
        
        @Override
        public boolean peek(Key key) {
            return apply(key, false);
        }
        
        private boolean apply(Key key, boolean update) {
            final DatawaveKey datawaveKey = new DatawaveKey(key);
            final String keyFieldName = JexlASTHelper.deconstructIdentifier(datawaveKey.getFieldName(), false);
            
            if (fieldName.equals(keyFieldName)) {
                if (acceptAll) {
                    return true;
                }
                
                final String keyFieldValue = datawaveKey.getFieldValue();
                final Set<Type> types = EventDataQueryExpressionVisitor.extractTypes(attributeFactory, keyFieldName, keyFieldValue, key);
                // always add the NoOpType to ensure the original value gets propagated through
                types.add(new NoOpType(keyFieldValue));
                final Set<Matcher> normalizedPatternMatchers = new HashSet<>();
                final Set<String> normalizedFieldValues = new HashSet<>();
                final Set<LiteralRange> normalizedRanges = new HashSet<>();
                for (Type type : types) {
                    // normalize all patterns
                    for (Pattern fieldPattern : fieldPatterns.keySet()) {
                        try {
                            String normalizedPattern = type.normalizeRegex(fieldPattern.toString());
                            if (normalizedPattern != null) {
                                normalizedPatternMatchers.add(Pattern.compile(normalizedPattern).matcher(EMPTY_STRING));
                            } else {
                                // can't normalize so add the original matcher
                                normalizedPatternMatchers.add(fieldPatterns.get(fieldPattern));
                            }
                        } catch (Exception e) {
                            // can't normalize this pattern, add the original matcher
                            normalizedPatternMatchers.add(fieldPatterns.get(fieldPattern));
                        }
                    }
                    
                    // normalize all values
                    for (String fieldValue : fieldValues) {
                        try {
                            String normalizedValue = type.normalize(fieldValue);
                            if (normalizedValue != null) {
                                normalizedFieldValues.add(normalizedValue);
                            } else {
                                // can't normalize this value, add the original
                                normalizedFieldValues.add(fieldValue);
                            }
                        } catch (Exception e) {
                            // can't normalize this value, add the original
                            normalizedFieldValues.add(fieldValue);
                        }
                    }
                    
                    // normalize all ranges
                    for (LiteralRange range : fieldRanges) {
                        try {
                            LiteralRange normalizedRange = new LiteralRange(range.getFieldName(), range.getNodeOperand());
                            
                            String normalizedLower = type.normalize(range.getLower().toString());
                            String normalizedUpper = type.normalize(range.getUpper().toString());
                            
                            if (normalizedLower != null && normalizedUpper != null) {
                                normalizedRange.updateLower(normalizedLower, range.isLowerInclusive(), range.getLowerNode());
                                normalizedRange.updateUpper(normalizedUpper, range.isUpperInclusive(), range.getUpperNode());
                                
                                normalizedRanges.add(normalizedRange);
                            } else {
                                // can't normalize the range values, add the original
                                normalizedRanges.add(range);
                            }
                        } catch (Exception e) {
                            // can't normalize the range values, add the original
                            normalizedRanges.add(range);
                        }
                    }
                }
                
                Set<String> fieldValuesToEvaluate = EventDataQueryExpressionVisitor.extractNormalizedValues(types);
                
                for (String normalizedFieldValue : fieldValuesToEvaluate) {
                    if (normalizedFieldValues.contains(normalizedFieldValue)) {
                        // field name matches and field value matches, keep.
                        if (update) {
                            nonNullValueSeen = true;
                        }
                        return true;
                    }
                    
                    for (Matcher m : normalizedPatternMatchers) {
                        m.reset(normalizedFieldValue);
                        if (m.matches()) {
                            // field name matches and field pattern matches, keep.
                            if (update) {
                                nonNullValueSeen = true;
                            }
                            return true;
                        }
                    }
                    
                    for (LiteralRange r : normalizedRanges) {
                        if (r.contains(normalizedFieldValue)) {
                            // field name patches and value is within range, keep.
                            if (update) {
                                nonNullValueSeen = true;
                            }
                            return true;
                        }
                    }
                    
                    if (nullValueFlag && !nonNullValueSeen) {
                        // field name has a nullValueFlag, keep one and only one instance
                        // of this field. (The fact of its presence will be sufficient
                        // to satisfy the null check condition assuming all other conditions are met
                        if (update) {
                            nonNullValueSeen = true;
                        }
                        return true;
                    }
                }
            }
            
            // field name does not match any of the rules above, reject this key.
            return false;
            
        }
        
        /**
         * A helper method to clone a set of filters
         * 
         * @param filters
         * @return a cloned set of filters
         */
        public static Map<String,? extends PeekingPredicate<Key>> clone(Map<String,? extends PeekingPredicate<Key>> filters) {
            Map<String,PeekingPredicate<Key>> cloned = new HashMap<>();
            for (Map.Entry<String,? extends PeekingPredicate<Key>> entry : filters.entrySet()) {
                if (entry.getValue() instanceof ExpressionFilter) {
                    cloned.put(entry.getKey(), ((ExpressionFilter) entry.getValue()).clone());
                } else {
                    cloned.put(entry.getKey(), entry.getValue());
                }
            }
            return cloned;
        }
        
        /**
         * A helper method to reset a set of filters
         *
         * @param filters
         */
        public static void reset(Map<String,? extends PeekingPredicate<Key>> filters) {
            for (Map.Entry<String,? extends PeekingPredicate<Key>> entry : filters.entrySet()) {
                if (entry.getValue() instanceof ExpressionFilter) {
                    ((ExpressionFilter) entry.getValue()).reset();
                }
            }
        }
    }
    
    /**
     *
     * @param script
     *            The query that will be used to generate the set of expression filters
     * @param factory
     *            An AttributeFactory used when generating normalized attributes.
     * @return A Map of field name to ExpressionFilter, suitable for selecting a set of Keys necessary to evaluate a query.
     */
    public static Map<String,ExpressionFilter> getExpressionFilters(ASTJexlScript script, AttributeFactory factory) {
        final EventDataQueryExpressionVisitor v = new EventDataQueryExpressionVisitor(factory);
        
        script.jjtAccept(v, "");
        
        return v.getFilterMap();
    }
    
    /**
     *
     * @param node
     *            the node that should be used to build the expression filters
     * @param factory
     * @return
     */
    public static Map<String,ExpressionFilter> getExpressionFilters(JexlNode node, AttributeFactory factory) {
        final EventDataQueryExpressionVisitor v = new EventDataQueryExpressionVisitor(factory);
        
        node.jjtAccept(v, "");
        
        return v.getFilterMap();
    }
    
    private final AttributeFactory attributeFactory;
    private final Map<String,ExpressionFilter> filterMap;
    
    private EventDataQueryExpressionVisitor(AttributeFactory factory) {
        this.attributeFactory = factory;
        this.filterMap = new HashMap<>();
    }
    
    private Map<String,ExpressionFilter> getFilterMap() {
        return this.filterMap;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        simpleValueFilter(node);
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        simpleValueFilter(node);
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        simplePatternFilter(node);
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        simplePatternFilter(node);
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        List<JexlNode> otherNodes = new ArrayList<>();
        LiteralRange range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            simpleRangeFilter(range);
        } else {
            super.visit(node, data);
        }
        
        return null;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        desc.addFilters(attributeFactory, filterMap);
        
        return null;
    }
    
    protected void simpleValueFilter(JexlNode node) {
        final JexlASTHelper.IdentifierOpLiteral iol = JexlASTHelper.getIdentifierOpLiteral(node);
        generateValueFilter(iol, false);
    }
    
    protected void simplePatternFilter(JexlNode node) {
        final JexlASTHelper.IdentifierOpLiteral iol = JexlASTHelper.getIdentifierOpLiteral(node);
        generateValueFilter(iol, true);
    }
    
    protected void simpleRangeFilter(LiteralRange<?> range) {
        final String fieldName = JexlASTHelper.deconstructIdentifier(range.getFieldName(), false);
        ExpressionFilter f = filterMap.get(fieldName);
        if (f == null) {
            filterMap.put(fieldName, f = createExpressionFilter(fieldName));
        }
        
        f.addFieldRange(range);
    }
    
    protected void generateValueFilter(JexlASTHelper.IdentifierOpLiteral iol, boolean isPattern) {
        if (iol != null) {
            final String fieldName = JexlASTHelper.deconstructIdentifier(iol.getIdentifier().image, false);
            ExpressionFilter f = filterMap.get(fieldName);
            if (f == null) {
                filterMap.put(fieldName, f = createExpressionFilter(fieldName));
            }
            
            final Object fieldValue = iol.getLiteralValue();
            
            if (fieldValue != null) {
                final String fieldStr = fieldValue.toString();
                
                if (isPattern) {
                    f.addFieldPattern(fieldStr);
                } else {
                    f.addFieldValue(fieldStr);
                }
            } else {
                f.setNullValueFlag();
            }
            
        } else {
            throw new NullPointerException("Null IdentifierOpLiteral");
        }
    }
    
    protected ExpressionFilter createExpressionFilter(String fieldName) {
        return new ExpressionFilter(attributeFactory, fieldName);
    }
    
    private static String print(JexlASTHelper.IdentifierOpLiteral iol) {
        if (iol == null)
            return "null";
        
        return iol.getIdentifier() + " " + iol.getOp() + " " + iol.getLiteral();
    }
    
    public static Set<Type> extractTypes(AttributeFactory attrFactory, String fieldName, String fieldValue, Key key) {
        final Set<Type> types = new HashSet<>();
        
        final Queue<Attribute<?>> attrQueue = new LinkedList<>();
        attrQueue.add(attrFactory.create(fieldName, fieldValue, key, true));
        
        Attribute<?> attr;
        
        while ((attr = attrQueue.poll()) != null) {
            if (TypeAttribute.class.isAssignableFrom(attr.getClass())) {
                TypeAttribute dta = (TypeAttribute) attr;
                Type t = dta.getType();
                types.add(t);
            } else if (AttributeBag.class.isAssignableFrom(attr.getClass())) {
                attrQueue.addAll(((AttributeBag<?>) attr).getAttributes());
            } else {
                log.warn("Unexpected attribute type when extracting type: " + attr.getClass().getCanonicalName());
            }
        }
        return types;
    }
    
    public static Set<String> extractNormalizedValues(Set<Type> types) {
        final Set<String> normalizedValues = new HashSet<>();
        
        for (Type type : types) {
            normalizedValues.add(type.getNormalizedValue());
        }
        
        return normalizedValues;
    }
}
