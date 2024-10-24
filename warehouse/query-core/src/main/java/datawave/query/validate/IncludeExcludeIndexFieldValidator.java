package datawave.query.validate;

import com.google.common.collect.Sets;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.visitors.FetchFunctionFieldsVisitor;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class IncludeExcludeIndexFieldValidator implements QueryValidator {
    
    private static final Set<String> supportedSyntaxes = Collections.singleton("JEXL");
    
    private static final Set<Pair<String,String>> functions = Collections.unmodifiableSet(
                    Sets.newHashSet(Pair.of(EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE,
                                                    EvaluationPhaseFilterFunctionsDescriptor.INCLUDE_REGEX),
                                    Pair.of(EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE,
                                                    EvaluationPhaseFilterFunctionsDescriptor.EXCLUDE_REGEX)));
    
    @Override
    public Set<String> getSupportedQuerySyntaxes() {
        return supportedSyntaxes;
    }
    
    @Override
    public List<String> evaluate(String query, String querySyntax, GenericQueryConfiguration config, MetadataHelper metadataHelper) throws Exception {
        if (supportedSyntaxes.contains(querySyntax)) {
            throw new IllegalArgumentException("Query syntax " + querySyntax + " is not supported for this evaluator.");
        }
        
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(query);
        Set<FetchFunctionFieldsVisitor.FunctionFields> foundFields = FetchFunctionFieldsVisitor.fetchFields(queryTree, functions, metadataHelper);
        List<String> messages = new ArrayList<>();
        if (!foundFields.isEmpty()) {
            Set<String> indexedFields = metadataHelper.getIndexedFields(null);
            for (FetchFunctionFieldsVisitor.FunctionFields functionFields : foundFields) {
                Set<String> intersection = Sets.intersection(indexedFields, functionFields.getFields());
                if (!intersection.isEmpty()) {
                    messages.add("Indexed fields found within the function " + functionFields.getNamespace() + ":" + functionFields.getFunction() + ": "
                                    + String.join(", ", intersection));
                }
            }
        }
        return messages;
    }
}
