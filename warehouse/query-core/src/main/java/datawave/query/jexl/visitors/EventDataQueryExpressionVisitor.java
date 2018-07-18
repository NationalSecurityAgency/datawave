package datawave.query.jexl.visitors;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.attributes.*;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.postprocessing.tf.Function;
import datawave.query.predicate.Filter;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.*;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    public static class ExpressionFilter implements Predicate<Key> {
        final AttributeFactory attributeFactory;
        
        /** The field this filter apples to */
        final String fieldName;
        /** fieldValues contains a set of literal values for which we need to keep data in order to satisfy the query */
        final Set<String> fieldValues;
        /** fieldPatterns contains a set of patterns for which we need to keep data in order to satisfy the query */
        final Set<Matcher> fieldPatterns;
        /** fieldRanges contains a set of ranges for which we need to keep data in order to satisfy the query */
        final Set<LiteralRange<?>> fieldRanges;
        /**
         * nullValueFlag indicates that we need to capture at least one instance of the field in order to satisfy a null value check in the query. It reset to
         * false once a single instance of the field is collected.
         */
        final AtomicBoolean nullValueFlag;
        
        final AtomicBoolean acceptAll;
        
        public ExpressionFilter(AttributeFactory attributeFactory, String fieldName) {
            this.attributeFactory = attributeFactory;
            this.fieldName = fieldName;
            this.fieldValues = new HashSet<>();
            this.fieldPatterns = new HashSet<>();
            this.fieldRanges = new HashSet<>();
            this.nullValueFlag = new AtomicBoolean(false);
            this.acceptAll = new AtomicBoolean(false);
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
            nullValueFlag.set(true);
        }
        
        public void addFieldPattern(String pattern) {
            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher("");
            fieldPatterns.add(m);
        }
        
        public void addFieldRange(LiteralRange range) {
            fieldRanges.add(range);
        }
        
        public void acceptAllValues() {
            acceptAll.set(true);
        }
        
        /**
         *
         * @param key
         *            the key to evaluate, must be parsable by DatawaveKey
         * @return true if the key should be kept in order to evaluare the query, false otherwise
         */
        public boolean apply(Key key) {
            final DatawaveKey datawaveKey = new DatawaveKey(key);
            final String keyFieldName = JexlASTHelper.deconstructIdentifier(datawaveKey.getFieldName(), false);
            
            if (fieldName.equals(keyFieldName)) {
                if (acceptAll.get()) {
                    return true;
                }
                
                final String keyFieldValue = datawaveKey.getFieldValue();
                final Set<String> normalizedFieldValues = EventDataQueryExpressionVisitor.extractNormalizedAttributes(attributeFactory, keyFieldName,
                                keyFieldValue, key);
                
                for (String normalizedFieldValue : normalizedFieldValues) {
                    if (fieldValues.contains(normalizedFieldValue)) {
                        // field name matches and field value matches, keep.
                        nullValueFlag.set(false);
                        return true;
                    }
                    
                    for (Matcher m : fieldPatterns) {
                        m.reset(normalizedFieldValue);
                        if (m.matches()) {
                            // field name matches and field pattern matches, keep.
                            nullValueFlag.set(false);
                            return true;
                        }
                    }
                    
                    for (LiteralRange r : fieldRanges) {
                        if (r.contains(normalizedFieldValue)) {
                            // field name patches and value is within range, keep.
                            nullValueFlag.set(false);
                            return true;
                        }
                    }
                    
                    if (nullValueFlag.compareAndSet(true, false)) {
                        // field name has a nullValueFlag, keep one and only one instance
                        // of this field. (The fact of its presence will be sufficient
                        // to satisfy the null check condition assuming all other conditions are met
                        return true;
                    }
                }
            }
            
            // field name does not match any of the rules above, reject this key.
            return false;
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
        List<JexlNode> otherNodes = new ArrayList<JexlNode>();
        Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic(node, otherNodes, true);
        if (ranges.size() == 1 && otherNodes.isEmpty()) {
            LiteralRange<?> range = ranges.keySet().iterator().next();
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
    
    public static Set<String> extractNormalizedAttributes(AttributeFactory attrFactory, String fieldName, String fieldValue, Key key) {
        final Set<String> normalizedAttributes = new HashSet<String>();
        
        final Queue<Attribute<?>> attrQueue = new LinkedList<>();
        attrQueue.add(attrFactory.create(fieldName, fieldValue, key, true));
        
        Attribute<?> attr;
        
        while ((attr = attrQueue.poll()) != null) {
            if (TypeAttribute.class.isAssignableFrom(attr.getClass())) {
                TypeAttribute dta = (TypeAttribute) attr;
                Type t = dta.getType();
                normalizedAttributes.add(t.getNormalizedValue());
            } else if (AttributeBag.class.isAssignableFrom(attr.getClass())) {
                attrQueue.addAll(((AttributeBag<?>) attr).getAttributes());
            } else {
                log.warn("Unexpected attribute type when extracting normalized values: " + attr.getClass().getCanonicalName());
            }
        }
        return normalizedAttributes;
    }
}
