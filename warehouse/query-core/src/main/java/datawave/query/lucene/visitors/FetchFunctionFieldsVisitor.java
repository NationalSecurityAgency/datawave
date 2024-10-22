package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class FetchFunctionFieldsVisitor extends BaseVisitor {
    
    private final Set<String> functions;
    
    public static Set<String> fetchFields(QueryNode node) {
        return fetchFields(node, Collections.emptySet());
    }
    
    public static Set<String> fetchFields(QueryNode node, Set<String> functions) {
        FetchFunctionFieldsVisitor visitor = new FetchFunctionFieldsVisitor(functions);
        Set<String> fields = new HashSet<>();
        return (Set<String>) visitor.visit(node, fields);
    }
    
    private FetchFunctionFieldsVisitor(Set<String> functions) {
        this.functions = functions.stream().map(String::toUpperCase).collect(Collectors.toSet());
    }
    
    @Override
    public Object visit(FunctionQueryNode node, Object data) {
        String function = node.getFunction().toUpperCase();
        Set<String> fields = (Set<String>) data;
        if (isTargetFunction(node.getFunction())) {
            switch (function) {
                
                default:
            }
        }
        return fields;
    }
    
    private boolean isTargetFunction(String function) {
        return functions.isEmpty() || functions.contains(function);
    }
}
