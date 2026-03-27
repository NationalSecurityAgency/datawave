package datawave.query.index.day;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.ShortCircuitBaseVisitor;

/**
 * Simple visitor that extracts all indexed, non-null literal equality terms from a query
 */
public class IndexedTermVisitor extends ShortCircuitBaseVisitor {

    private final Set<String> indexedFields;
    private final Multimap<String,String> fieldsAndValues;

    public static Multimap<String,String> getIndexedFieldsAndValues(JexlNode node, Set<String> indexedFields) {
        IndexedTermVisitor visitor = new IndexedTermVisitor(indexedFields);
        node.jjtAccept(visitor, null);
        return visitor.getFieldsAndValues();
    }

    public IndexedTermVisitor(Set<String> indexedFields) {
        this.fieldsAndValues = HashMultimap.create();
        this.indexedFields = indexedFields;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        if (field != null && indexedFields.contains(field)) {
            String value = (String) JexlASTHelper.getLiteralValueSafely(node);
            if (value != null) {
                fieldsAndValues.put(field, value);
            }
        }
        return data;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            return data;
        } else {
            return super.visit(node, data);
        }
    }

    public Multimap<String,String> getFieldsAndValues() {
        return fieldsAndValues;
    }
}
