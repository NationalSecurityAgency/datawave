package datawave.query.postprocessing.tf;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.ContentFunctionsDescriptor;
import datawave.query.jexl.functions.ContentFunctionsDescriptor.ContentJexlArgumentDescriptor;

/**
 * A function that intersects document keys prior to fetching term offsets.
 * <p>
 * This only works on positive phrase queries -- that is, no guarantee is made for negated phrases
 */
public class DocumentKeysFunction {

    private static final Logger log = Logger.getLogger(DocumentKeysFunction.class);

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
        ContentFunctionsDescriptor.FieldTerms fieldsAndTerms;
        JexlNode parent;
        String field;

        Multimap<String,Function> functions = TermOffsetPopulator.getContentFunctions(node);
        for (String key : functions.keySet()) {
            Collection<Function> coll = functions.get(key);
            for (Function f : coll) {
                parent = f.args().get(0).jjtGetParent().jjtGetParent();

                if (!(parent instanceof ASTFunctionNode)) {
                    throw new IllegalArgumentException("parent was not a function node");
                }

                argsDescriptor = descriptor.getArgumentDescriptor((ASTFunctionNode) parent);

                // content, tf, and indexed fields are not actually needed to extract fields from the function node
                fieldsAndTerms = argsDescriptor.fieldsAndTerms(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);

                if (fieldsAndTerms.totalFields() != 1) {
                    throw new IllegalStateException("content function had more than one field");
                }

                field = JexlASTHelper.deconstructIdentifier(fieldsAndTerms.getFields().iterator().next());
                ContentFunction contentFunction = new ContentFunction(field, fieldsAndTerms.getTerms());
                contentFunctions.put(contentFunction.getField(), contentFunction);

                if (isFunctionNegated(f)) {
                    negatedValues.addAll(contentFunction.getValues());
                }
            }
        }
    }

    public boolean isFunctionNegated(Function f) {
        JexlNode node = f.args().get(0);
        int numNegations = 0;
        if (node != null) {
            while (node.jjtGetParent() != null) {
                node = node.jjtGetParent();
                if (node instanceof ASTNotNode) {
                    numNegations++;
                }
            }
        }
        return numNegations % 2 == 1;
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

        // add all negated values
        for (String value : negatedValues) {
            valueToKeys.putAll(value, docKeys);
        }

        Set<Key> filterKeys = buildFilterKeys(valueToKeys);

        if (log.isDebugEnabled() && docKeys.size() != filterKeys.size()) {
            log.debug("reduced document keys from " + docKeys.size() + " to " + filterKeys.size() + " (-" + (docKeys.size() - filterKeys.size()) + ")");
        }

        return filterKeys;
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
        for (String key : contentFunctions.keySet()) {
            Attribute<?> attr = d.get(key);
            if (attr != null) {
                Collection<String> values = getValuesForKey(key, contentFunctions);
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
    private Set<String> getValuesForKey(String key, Multimap<String,ContentFunction> functions) {
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
                    putValueKey(att, valueToKeys, values);
                }
            }
        } else if (attr instanceof TypeAttribute) {
            // special case of a TypeAttribute
            final String value = ((TypeAttribute<?>) attr).getType().getDelegateAsString();
            if (values.contains(value)) {
                valueToKeys.put(value, attr.getMetadata());
            }
        } else if (values.contains(String.valueOf(attr.getData()))) {
            valueToKeys.put((String) attr.getData(), attr.getMetadata());
        }
    }

    /**
     * Builds the filter keys from the content functions and value-to-keys multimap
     * <p>
     * The filter keys are effectively the union of each content function's intersection
     *
     * @param valueToKeys
     *            a multimap of value to document keys
     * @return a set of filter keys
     */
    private Set<Key> buildFilterKeys(Multimap<String,Key> valueToKeys) {
        Set<Key> filterKeys = new HashSet<>();
        for (ContentFunction function : contentFunctions.values()) {
            Set<Key> keys = null;
            for (String value : function.getValues()) {
                if (keys == null) {
                    keys = new HashSet<>(valueToKeys.get(value));
                } else {
                    keys.retainAll(valueToKeys.get(value));
                }

                if (keys.isEmpty())
                    break;
            }

            if (keys != null) {
                filterKeys.addAll(keys);
            }
        }
        return filterKeys;
    }
}
