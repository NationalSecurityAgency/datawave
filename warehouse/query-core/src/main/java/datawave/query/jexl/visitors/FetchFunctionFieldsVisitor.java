package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FetchFunctionFieldsVisitor extends BaseVisitor {
    
    private final Set<Pair<String,String>> functions = new HashSet<>();
    private final HashMultimap<Pair<String,String>,String> fields = HashMultimap.create();
    
    public static Set<FunctionFields> fetchFields(JexlNode node, Set<Pair<String, String>> functions) {
        if (node != null) {
            FetchFunctionFieldsVisitor visitor = new FetchFunctionFieldsVisitor(functions);
            node.jjtAccept(visitor, functions);
            return visitor.getFunctionFields();
        } else {
            return Collections.emptySet();
        }
    }
    
    private FetchFunctionFieldsVisitor(Set<Pair<String,String>> functions) {
        functions.forEach((p) -> this.functions.add(Pair.of(p.getLeft(), p.getRight())));
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        node.jjtAccept(visitor, null);
        
        Pair<String,String> function = Pair.of(visitor.namespace(), visitor.name());
        if (functions.contains(function)) {
        
        }
        return super.visit(node, data);
    }
    
    private Set<FunctionFields> getFunctionFields() {
        Set<FunctionFields> functionFields = new HashSet<>();
        for (Pair<String, String> pair : fields.keys()) {
            functionFields.add(new FunctionFields(pair.getLeft(), pair.getRight(), fields.get(pair)));
        }
        
        for (Pair<String, String> pair : this.functions) {
            if (this.fields.containsKey(pair)) {
                functionFields.add(new FunctionFields(pair.getKey(), pair.getLeft(), this.fields.get(pair)));
            } else {
                functionFields.add(new FunctionFields(pair.getKey(), pair.getLeft()));
            }
        }
        return functionFields;
    }
    
    public static class FunctionFields {
        private final String namespace;
        private final String function;
        private final Set<String> fields;
        
        private FunctionFields(String namespace, String function) {
            this(namespace, function, Collections.emptySet());
        }
        
        private FunctionFields(String namespace, String function, Set<String> fields) {
            this.namespace = namespace;
            this.function = function;
            this.fields = fields.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(fields);
        }
        
        public String getNamespace() {
            return namespace;
        }
        
        public String getFunction() {
            return function;
        }
        
        public Set<String> getFields() {
            return fields;
        }
    }
}
