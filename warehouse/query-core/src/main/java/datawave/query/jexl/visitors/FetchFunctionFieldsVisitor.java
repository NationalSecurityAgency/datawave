package datawave.query.jexl.visitors;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.LinkedHashMultimap;

import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;

/**
 * A visitor that fetches all fields from the specified functions.
 */
public class FetchFunctionFieldsVisitor extends ShortCircuitBaseVisitor {

    private final Set<Pair<String,String>> functions;
    private final MetadataHelper metadataHelper;
    // Maintain insertion order.
    private final LinkedHashMultimap<Pair<String,String>,String> fields = LinkedHashMultimap.create();

    /**
     * Fetch the fields seen in the specified functions.
     *
     * @param query
     *            the query tree
     * @param functions
     *            the set of {@code <namespace, function>} pairs to filter on
     * @param metadataHelper
     *            the metadata helper
     * @return the set of fields found within the functions
     */
    public static Set<FunctionFields> fetchFields(JexlNode query, Set<Pair<String,String>> functions, MetadataHelper metadataHelper) {
        if (query != null) {
            FetchFunctionFieldsVisitor visitor = new FetchFunctionFieldsVisitor(functions, metadataHelper);
            query.jjtAccept(visitor, functions);
            return visitor.getFunctionFields();
        } else {
            return Collections.emptySet();
        }
    }

    private FetchFunctionFieldsVisitor(Set<Pair<String,String>> functions, MetadataHelper metadataHelper) {
        if (functions == null || functions.isEmpty()) {
            this.functions = Collections.emptySet();
        } else {
            this.functions = new HashSet<>();
            functions.forEach((p) -> this.functions.add(Pair.of(p.getLeft(), p.getRight())));
        }
        this.metadataHelper = metadataHelper;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        node.jjtAccept(visitor, null);

        Pair<String,String> function = Pair.of(visitor.namespace(), visitor.name());
        // If we are either not filtering out functions, or the function filters contains the functions, fetch the fields.
        if (functions.isEmpty() || functions.contains(function)) {
            JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
            Set<String> fields = desc.fields(metadataHelper, null);
            // Add the fields to the function.
            if (!fields.isEmpty()) {
                this.fields.putAll(function, fields);
            }
        }
        return null;
    }

    // Returns the fields map as a set of FunctionFields.
    private Set<FunctionFields> getFunctionFields() {
        // Maintain insertion order.
        Set<FunctionFields> functionFields = new LinkedHashSet<>();
        for (Pair<String,String> function : fields.keySet()) {
            functionFields.add(new FunctionFields(function.getLeft(), function.getRight(), fields.get(function)));
        }
        return functionFields;
    }

    public static class FunctionFields {
        private final String namespace;
        private final String function;
        private final Set<String> fields;

        public static FunctionFields of(String namespace, String function, String... fields) {
            return new FunctionFields(namespace, function, Arrays.asList(fields));
        }

        private FunctionFields(String namespace, String function, Collection<String> fields) {
            this.namespace = namespace;
            this.function = function;
            // Maintain insertion order.
            this.fields = fields.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(new LinkedHashSet<>(fields));
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

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            FunctionFields that = (FunctionFields) object;
            return Objects.equals(namespace, that.namespace) && Objects.equals(function, that.function) && Objects.equals(fields, that.fields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, function, fields);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", FunctionFields.class.getSimpleName() + "[", "]").add("namespace='" + namespace + "'")
                            .add("function='" + function + "'").add("fields=" + fields).toString();
        }
    }
}
