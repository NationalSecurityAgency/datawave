package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.ContentFunctionsDescriptor;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.GeoWaveFunctionsDescriptor;
import datawave.query.jexl.functions.GroupingRequiredFilterFunctionsDescriptor;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.functions.QueryFunctionsDescriptor;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static datawave.query.jexl.functions.ContentFunctions.CONTENT_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.ContentFunctionsDescriptor.ContentJexlArgumentDescriptor;
import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor.EvaluationPhaseFilterJexlArgumentDescriptor;
import static datawave.query.jexl.functions.GeoWaveFunctions.GEOWAVE_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor;
import static datawave.query.jexl.functions.GroupingRequiredFilterFunctions.GROUPING_REQUIRED_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.GroupingRequiredFilterFunctionsDescriptor.GroupingRequiredFilterJexlArgumentDescriptor;
import static datawave.query.jexl.functions.QueryFunctionsDescriptor.QueryJexlArgumentDescriptor;

/**
 * Visitor that answers several basic questions about fields in a query plan
 * <ul>
 * <li>is a field indexed, index-only, or tokenized?</li>
 * <li>is a field part of a function such a content, filter, grouping or query function?</li>
 * <li>is a field found in regex terms?</li>
 * </ul>
 */
public class QueryFieldMetadataVisitor extends BaseVisitor {

    private static final Logger log = Logger.getLogger(QueryFieldMetadataVisitor.class);

    private final Set<String> indexedFields;
    private final Set<String> indexOnlyFields;
    private final Set<String> tokenizedFields;

    private final Set<String> contentFunctionFields = new HashSet<>();
    private final Set<String> filterFunctionFields = new HashSet<>();
    private final Set<String> queryFunctionFields = new HashSet<>();
    private final Set<String> geoFunctionFields = new HashSet<>();
    private final Set<String> groupingFunctionFields = new HashSet<>();
    private final Set<String> regexFields = new HashSet<>();

    public QueryFieldMetadataVisitor(Set<String> indexedFields, Set<String> indexOnlyFields, Set<String> tokenizedFields) {
        this.indexedFields = indexedFields;
        this.indexOnlyFields = indexOnlyFields;
        this.tokenizedFields = tokenizedFields;
    }

    // base methods

    public boolean isFieldIndexed(String field) {
        return indexedFields.contains(field);
    }

    public boolean isFieldIndexOnly(String field) {
        return indexOnlyFields.contains(field);
    }

    public boolean isFieldTokenized(String field) {
        return tokenizedFields.contains(field);
    }

    public boolean isFieldAnyFunction(String field) {
        //  @formatter:off
        return contentFunctionFields.contains(field) ||
                        filterFunctionFields.contains(field) ||
                        queryFunctionFields.contains(field) ||
                        geoFunctionFields.contains(field) ||
                        groupingFunctionFields.contains(field);
        //  @formatter:off
    }

    public boolean isFieldContentFunction(String field){
        return contentFunctionFields.contains(field);
    }

    public boolean isFieldFilterFunction(String field){
        return filterFunctionFields.contains(field);
    }

    public boolean isFieldQueryFunction(String field){
        return queryFunctionFields.contains(field);
    }

    public boolean isFieldGeoFunction(String field){
        return geoFunctionFields.contains(field);
    }

    public boolean isFieldGroupingFunction(String field) {
        return groupingFunctionFields.contains(field);
    }

    public boolean isFieldRegexTerm(String field){
        return regexFields.contains(field);
    }

    //  visitor methods

    //  leaf nodes that we care about

    @Override
    public Object visit(ASTERNode node, Object data) {
        regexFields.add(parseSingleField(node));
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        regexFields.add(parseSingleField(node));
        return data;
    }

    //  the fun one
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        parseFieldsForFunction(node);
        return data;
    }

    //  helper methods

    private String parseSingleField(JexlNode node){
        return JexlASTHelper.getIdentifier(node);
    }

    private void parseFieldsForFunction(ASTFunctionNode node) {
        FunctionJexlNodeVisitor visitor = FunctionJexlNodeVisitor.eval(node);

        switch (visitor.namespace()) {
            case CONTENT_FUNCTION_NAMESPACE:
                //  all content function fields are added
                ContentJexlArgumentDescriptor contentDescriptor = new ContentFunctionsDescriptor().getArgumentDescriptor(node);
                contentFunctionFields.addAll(contentDescriptor.fieldsAndTerms(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null)[0]);
                break;
            case EVAL_PHASE_FUNCTION_NAMESPACE:
                //  might be able to exclude certain evaluation phase functions from this step
                EvaluationPhaseFilterJexlArgumentDescriptor evaluationDescriptor = (EvaluationPhaseFilterJexlArgumentDescriptor) new EvaluationPhaseFilterFunctionsDescriptor().getArgumentDescriptor(node);
                Set<String> evaluationFields = evaluationDescriptor.fields(null, Collections.emptySet());
                filterFunctionFields.addAll(evaluationFields);
                break;
            case GEOWAVE_FUNCTION_NAMESPACE:
                GeoWaveJexlArgumentDescriptor geoDescriptor = (GeoWaveJexlArgumentDescriptor) new GeoWaveFunctionsDescriptor().getArgumentDescriptor(node);
                Set<String> geoFields = geoDescriptor.fields(null, Collections.emptySet());
                geoFunctionFields.addAll(geoFields);
                break;
            case GROUPING_REQUIRED_FUNCTION_NAMESPACE:
                GroupingRequiredFilterJexlArgumentDescriptor groupingDescriptor = (GroupingRequiredFilterJexlArgumentDescriptor) new GroupingRequiredFilterFunctionsDescriptor().getArgumentDescriptor(node);
                Set<String> groupingFields = groupingDescriptor.fields(null, Collections.emptySet());
                groupingFunctionFields.addAll(groupingFields);
                break;
            case QueryFunctions.QUERY_FUNCTION_NAMESPACE:
                QueryJexlArgumentDescriptor queryDescriptor = (QueryJexlArgumentDescriptor) new QueryFunctionsDescriptor().getArgumentDescriptor(node);
                Set<String> queryFields = queryDescriptor.fields(null, Collections.emptySet());
                queryFunctionFields.addAll(queryFields);
                break;
            default:
                //  do nothing
                log.warn("Unhandled function namespace: " + visitor.namespace());
        }
    }

}
