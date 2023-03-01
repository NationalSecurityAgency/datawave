package datawave.query.postprocessing.tf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.ContentFunctionsDescriptor;
import datawave.query.jexl.functions.ContentFunctionsDescriptor.ContentJexlArgumentDescriptor;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static datawave.query.jexl.functions.ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_SCORED_PHRASE_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME;
import static datawave.query.jexl.functions.ContentFunctions.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME;

/**
 * A function that intersects document keys prior to fetching term offsets.
 * <p>
 * This only works on positive phrase queries -- that is, no guarantee is made for negated phrases
 */
public class DocumentKeysFunction {
    
    private static final Logger log = Logger.getLogger(DocumentKeysFunction.class);
    
    // cannot intersect on negated values
    private final Set<String> negatedValues = new HashSet<>();
    private final Multimap<String,ContentFunction> contentFunctions = LinkedListMultimap.create();
    
    /**
     * @param config
     *            a {@link TermFrequencyConfig}
     */
    public DocumentKeysFunction(TermFrequencyConfig config) {
        populateContentFunctions(config.getScript());
    }
    
    protected void populateContentFunctions(JexlNode node) {
        
        ContentFunctionsDescriptor descriptor = new ContentFunctionsDescriptor();
        ContentJexlArgumentDescriptor argsDescriptor;
        Set<String>[] fieldsAndTerms;
        JexlNode parent;
        
        Multimap<String,Function> functions = TermOffsetPopulator.getContentFunctions(node);
        for (String key : functions.keySet()) {
            Collection<Function> coll = functions.get(key);
            for (Function f : coll) {
                parent = f.args().get(0).jjtGetParent();
                
                if (!(parent instanceof ASTFunctionNode)) {
                    throw new IllegalArgumentException("parent was not a function node");
                }
                
                argsDescriptor = descriptor.getArgumentDescriptor((ASTFunctionNode) parent);
                
                // content, tf, and indexed fields are not actually needed to extract fields from the function node
                fieldsAndTerms = argsDescriptor.fieldsAndTerms(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);
                
                if (fieldsAndTerms[0].size() != 1) {
                    throw new IllegalStateException("content function had more than one field");
                }
                
                ContentFunction contentFunction = new ContentFunction(fieldsAndTerms[0].iterator().next(), fieldsAndTerms[1]);
                contentFunctions.put(contentFunction.getField(), contentFunction);
                
                // need to record any values that are part of a negative function
                if (isFunctionNegated(f)) {
                    negatedValues.addAll(contentFunction.getValues());
                }
            }
        }
    }
    
    private boolean isFunctionNegated(Function f) {
        JexlNode node = f.args().get(0);
        if (node != null) {
            while (node.jjtGetParent() != null) {
                node = node.jjtGetParent();
                if (node instanceof ASTNotNode) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private ContentFunction transformFunction(Function function) {
        
        boolean specialCase = false;
        int index = findValueStartIndex(function);
        
        // Parse the values first. Might have to lookup fields by value.
        TreeSet<String> values = getValuesFromArgs(function.args(), index);
        
        TreeSet<String> fields;
        if (specialCase) {
            // field(s) were not present in the function, and cannot lookup fields by value.
            throw new IllegalStateException("cannot lookup fields by value");
        } else {
            // function's first arg is the field, or fields in the form (FIELD_A || FIELD_B)
            fields = parseField(function.args().get(0));
        }
        
        return new ContentFunction(fields.first(), values);
    }
    
    // functions may take different forms..
    // within = {field, number, termOffsetMap, terms...}
    // within = {number, termOffsetMap, terms...}
    // adjacent = {field, termOffsetMap, terms...}
    // adjacent = {termOffsetMap, terms...}
    // phrase = {field, termOffsetMap, terms...}
    // phrase = {termOffsetMap, terms...}
    // scoredPhrase = {-1.5, termOffsetMap,'boy','car'}
    private int findValueStartIndex(Function function) {
        int index;
        switch (function.name()) {
            case CONTENT_WITHIN_FUNCTION_NAME:
                index = 3;
                break;
            case CONTENT_ADJACENT_FUNCTION_NAME:
            case CONTENT_PHRASE_FUNCTION_NAME:
            case CONTENT_SCORED_PHRASE_FUNCTION_NAME:
                index = 2;
                break;
            default:
                throw new IllegalStateException("unexpected function name: " + function.name());
        }
        
        // if the first node is a number of a 'termOffsetMap'
        if (isFirstNodeSpecial(function.args().get(0))) {
            index++;
        }
        return index;
    }
    
    // A node is special if it is not the fields being searched. That is, the first node is a number or a variable 'termOffsetMap'
    private boolean isFirstNodeSpecial(JexlNode node) {
        node = JexlASTHelper.dereference(node);
        if (node instanceof ASTNumberLiteral || node instanceof ASTUnaryMinusNode) {
            return true;
        } else if (node instanceof ASTIdentifier || node instanceof ASTOrNode) {
            List<ASTIdentifier> ids = JexlASTHelper.getIdentifiers(node);
            return ids.size() == 1 && ids.get(0).image.equals(TERM_OFFSET_MAP_JEXL_VARIABLE_NAME);
        }
        return false;
    }
    
    /**
     * Extract fields from a JexlNode. Node may be an OR node like (FIELD_A || FIELD_B).
     *
     * @param node
     *            a jexl node
     * @return a sorted set of fields
     */
    private TreeSet<String> parseField(JexlNode node) {
        TreeSet<String> fields = new TreeSet<>();
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            fields.add(JexlASTHelper.deconstructIdentifier(identifier));
        }
        return fields;
    }
    
    /**
     * Parse the values out of a content functions arguments
     *
     * @param args
     *            content function arguments as a list of Jexl nodes
     * @param start
     *            the start index for the values
     * @return a list of normalized string values
     */
    private TreeSet<String> getValuesFromArgs(List<JexlNode> args, int start) {
        TreeSet<String> values = new TreeSet<>();
        for (int i = start; i < args.size(); i++) {
            List<String> parsed = parseArg(args.get(i));
            values.addAll(parsed);
        }
        return values;
    }
    
    private List<String> parseArg(JexlNode node) {
        List<String> parsed = new LinkedList<>();
        List<Object> values = JexlASTHelper.getLiteralValues(node);
        for (Object value : values) {
            if (value instanceof String) {
                parsed.add((String) value);
            } else {
                throw new IllegalStateException("Expected literal value to be String cast-able");
            }
        }
        return parsed;
    }
    
    /**
     * Reduces the search space to the union of the document keys associated with each content function
     *
     * @param d
     *            the document
     * @param docKeys
     *            the set of keys associated with the {@link Document#DOCKEY_FIELD_NAME} field
     * @return a filtered set of document keys
     */
    protected Set<Key> getDocKeys(Document d, Set<Key> docKeys) {
        Multimap<String,Key> valueToKeys = buildValueToKeys(d);
        
        // each content function's intersected search space is added
        // along with the entire search space for any negated function
        Set<Key> filterKeys = buildFilterKeys(valueToKeys);
        
        int origSize = docKeys.size();
        docKeys.retainAll(filterKeys);
        int nextSize = docKeys.size();
        if (origSize != nextSize) {
            log.info("reduced document keys from " + origSize + " to " + nextSize + " (-" + (origSize - nextSize) + ")");
        }
        
        return docKeys;
    }
    
    /**
     * Constructs the multimap of value to document keys
     *
     * @param d
     *            a document
     * @return a multimap of value to document keys
     */
    private Multimap<String,Key> buildValueToKeys(Document d) {
        Multimap<String,Key> valueToKeys = HashMultimap.create();
        for (String field : contentFunctions.keySet()) {
            Attribute<?> attr = d.get(field);
            if (attr != null) {
                Collection<String> values = getValuesForField(field, contentFunctions);
                putValueKey(attr, valueToKeys, values);
            }
        }
        return valueToKeys;
    }
    
    /**
     * Gets the sum of all content function values
     *
     * @param key
     *            the field
     * @param functions
     *            the multimap of field to content functions
     * @return a union of all values
     */
    private Set<String> getValuesForField(String key, Multimap<String,ContentFunction> functions) {
        Set<String> values = new HashSet<>();
        for (ContentFunction f : functions.get(key)) {
            values.addAll(f.getValues());
        }
        return values;
    }
    
    /**
     * Extract value or values from the provided Attribute, only adding to the multimap if the values match the filter
     *
     * @param attr
     *            an {@link Attribute}
     * @param valueToKeys
     *            a multimap of value to document keys
     * @param values
     *            a set of values used as a filter
     */
    private void putValueKey(Attribute<?> attr, Multimap<String,Key> valueToKeys, Collection<String> values) {
        if (attr instanceof Attributes) {
            Attributes attrs = (Attributes) attr;
            for (Attribute<?> att : attrs.getAttributes()) {
                if (att instanceof PreNormalizedAttribute) {
                    PreNormalizedAttribute pre = (PreNormalizedAttribute) att;
                    if (values.contains(pre.getValue())) {
                        valueToKeys.put(pre.getValue(), pre.getMetadata());
                    }
                } else {
                    if (values.contains(att.getData())) {
                        valueToKeys.put((String) att.getData(), att.getMetadata());
                    }
                }
            }
        } else if (values.contains(attr.getData())) {
            valueToKeys.put((String) attr.getData(), attr.getMetadata());
        }
    }
    
    /**
     * Builds the filter keys from the content functions and value-to-keys multimap
     *
     * @param valueToKeys
     *            a multimap of value to document keys
     * @return a set of filter keys
     */
    private Set<Key> buildFilterKeys(Multimap<String,Key> valueToKeys) {
        Set<Key> filterKeys = new HashSet<>();
        for (ContentFunction function : contentFunctions.values()) {
            Set<Key> searchSpace = null;
            for (String value : function.getValues()) {
                if (searchSpace == null) {
                    searchSpace = new HashSet<>(valueToKeys.get(value));
                } else {
                    searchSpace.retainAll(valueToKeys.get(value));
                }
                
                if (searchSpace.isEmpty()) {
                    // if the keys ever intersect to zero, we're done
                    break;
                }
            }
            
            if (searchSpace != null) {
                filterKeys.addAll(searchSpace);
            }
        }
        
        // add in all keys for all negated values
        for (String value : negatedValues) {
            filterKeys.addAll(valueToKeys.get(value));
        }
        
        return filterKeys;
    }
}
