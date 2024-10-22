package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.AnyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ProximityQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

import java.util.HashSet;
import java.util.Set;

/**
 * Fetches all fields from a lucene query tree.
 */
public class FetchFieldsVisitor extends BaseVisitor {
    
    /**
     * Returns the set of unique fields found in the given query node.
     * @param node the node
     * @return the fields
     */
    public static Set<String> fetchFields(QueryNode node) {
        Set<String> fields = new HashSet<>();
        FetchFieldsVisitor visitor = new FetchFieldsVisitor();
        return (Set<String>) visitor.visit(node, fields);
    }
    
    @Override
    public Object visit(AnyQueryNode node, Object data) {
        return addFieldAndVisitChildren(node, data, node.getFieldAsString());
    }
    
    @Override
    public Object visit(FieldQueryNode node, Object data) {
        return addFieldAndVisitChildren(node, data, node.getFieldAsString());
    }
    
    @Override
    public Object visit(FuzzyQueryNode node, Object data) {
        return addFieldAndVisitChildren(node, data, node.getFieldAsString());
    }
    
    @Override
    public Object visit(ProximityQueryNode node, Object data) {
        return addFieldAndVisitChildren(node, data, node.getFieldAsString());
    }
    
    @Override
    public Object visit(QuotedFieldQueryNode node, Object data) {
        return addFieldAndVisitChildren(node, data, node.getFieldAsString());
    }
    
    @Override
    public Object visit(PointQueryNode node, Object data) {
        return addFieldAndVisitChildren(node, data, String.valueOf(node.getField()));
    }
    
    @Override
    public Object visit(PrefixWildcardQueryNode node, Object data) {
        return addFieldAndVisitChildren(node, data, node.getFieldAsString());
    }
    
    @Override
    public Object visit(RegexpQueryNode node, Object data) {
        return addFieldAndVisitChildren(node, data, node.getFieldAsString());
    }
    
    @Override
    public Object visit(WildcardQueryNode node, Object data) {
        return addFieldAndVisitChildren(node, data, node.getFieldAsString());
    }
    
    @Override
    public Object visit(FunctionQueryNode node, Object data) {
        Set<String> functionFields = FetchFunctionFieldsVisitor.fetchFields(node);
        Set<String> fields = (Set<String>) data;
        fields.addAll(functionFields);
        return fields;
    }
    
    private Object addFieldAndVisitChildren(QueryNode node, Object data, String field) {
        Set<String> fields = (Set<String>) data;
        fields.add(field);
        visitChildren(node, fields);
        return fields;
    }
}
