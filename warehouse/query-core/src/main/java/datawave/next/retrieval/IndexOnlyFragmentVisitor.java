package datawave.next.retrieval;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTEQNode;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.BaseVisitor;

/**
 * Visitor that collects the index only and tokenized field-value pairs required for document evaluation
 * <p>
 * Note: a field can be both index only and tokenized at the same time (e.g. an index-only field that is also tokenized for {@code content:phrase} support). A
 * term against such a field must be collected into both sets, since {@link DocumentIterator#collectIndexOnlyFragments} and
 * {@link DocumentIterator#collectTermFrequencyFragments} each populate a different part of the document required for evaluation.
 */
public class IndexOnlyFragmentVisitor extends BaseVisitor {

    private final Set<String> indexOnlyFields;
    private final Set<String> tokenizedFields;

    private final Multimap<String,String> indexOnlyFieldValues;
    private final Multimap<String,String> tokenizedFieldValues;

    public IndexOnlyFragmentVisitor(Set<String> indexOnlyFields, Set<String> tokenizedFields) {
        this.indexOnlyFields = indexOnlyFields;
        this.tokenizedFields = tokenizedFields;

        this.indexOnlyFieldValues = HashMultimap.create();
        this.tokenizedFieldValues = HashMultimap.create();
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        if (field == null) {
            return data;
        }

        Object literal = JexlASTHelper.getLiteralValue(node);
        if (literal == null) {
            return data;
        }

        if (tokenizedFields != null && tokenizedFields.contains(field)) {
            tokenizedFieldValues.put(field, String.valueOf(literal));
        }
        if (indexOnlyFields != null && indexOnlyFields.contains(field)) {
            indexOnlyFieldValues.put(field, String.valueOf(literal));
        }

        return data;
    }

    public Multimap<String,String> getIndexOnlyFieldValues() {
        return indexOnlyFieldValues;
    }

    public Multimap<String,String> getTokenizedFieldValues() {
        return tokenizedFieldValues;
    }
}
