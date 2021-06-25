package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.GeoWaveFunctionsDescriptor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.log4j.Logger;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor.INCLUDE_REGEX;

/**
 * The 'WhindexVisitor' is used to replace wide-scoped geowave fields with value-specific, narrow-scoped geowave fields where appropriate.
 * <p>
 * For example, assume you have a wide-scope geowave field (e.g. GEOWAVE_FIELD), a narrow-scope geowave field(s) (e.g. MARS_GEOWAVE_FIELD or
 * EARTH_GEOWAVE_FIELD), and a separate field whose value is specific to each narrow-scoped field (e.g. PLANET == 'MARS' or PLANET == 'EARTH').
 * <p>
 * If a query comes in with the following form: geowave:intersects(GEOWAVE_FIELD, '...some wkt...) &amp;&amp; PLANET == 'MARS'
 * <p>
 * The WhindexVisitor would turn that into this: geowave:intersects(MARS_GEOWAVE_FIELD, '...some wkt')
 * <p>
 * The update function uses the value-specific geowave field, and drops the anded term (since all of the data indexed under the value-specific geowave field
 * already satisfies that term).
 *
 */
public class WhindexVisitor extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(WhindexVisitor.class);
    
    private final Set<String> mappingFields;
    private final Set<String> fieldValues;
    private final Map<String,Map<String,List<String>>> valueSpecificFieldMappings;
    private final MetadataHelper metadataHelper;
    private final Set<String> functionFields;
    
    private WhindexVisitor(Set<String> mappingFields, Map<String,Map<String,List<String>>> valueSpecificFieldMappings, MetadataHelper metadataHelper) {
        this.mappingFields = mappingFields;
        this.valueSpecificFieldMappings = valueSpecificFieldMappings;
        this.metadataHelper = metadataHelper;
        this.fieldValues = new HashSet<>(this.valueSpecificFieldMappings.keySet());
        this.functionFields = this.valueSpecificFieldMappings.values().stream().flatMap(m -> m.keySet().stream()).collect(Collectors.toSet());
    }
    
    public static <T extends JexlNode> T apply(JexlNode node, Set<String> mappingFields, Map<String,Map<String,List<String>>> valueSpecificFieldMappings,
                    MetadataHelper metadataHelper) {
        WhindexVisitor visitor = new WhindexVisitor(mappingFields, valueSpecificFieldMappings, metadataHelper);
        return (T) TreeFlatteningRebuildingVisitor.flatten(node).jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        List<FunctionNodeInfo> functionNodeInfoList = new ArrayList<>();
        MultiValueMap<String,JexlNode> valueEqNodeMap = new LinkedMultiValueMap<>();
        Set<String> regexValueMatches = new HashSet<>();
        LinkedList<JexlNode> children = new LinkedList<>();
        
        // get all of the GeoWave nodes and field-mapping EQ nodes
        for (JexlNode child : JexlNodes.children(node)) {
            children.add(child);
            JexlNode dereferenced = JexlASTHelper.dereference(child);
            
            // get all of the GeoWave and include regex function nodes
            if (dereferenced instanceof ASTFunctionNode) {
                JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor((ASTFunctionNode) dereferenced);
                
                // if it's a GeoWave function
                if (descriptor instanceof GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor) {
                    Set<String> fields = descriptor.fields(metadataHelper, Collections.emptySet());
                    fields.retainAll(functionFields);
                    if (!fields.isEmpty()) {
                        // @formatter:off
                        functionNodeInfoList.add(
                                new FunctionNodeInfo(
                                        child,
                                        (ASTFunctionNode) dereferenced,
                                        descriptor,
                                        descriptor.fields(metadataHelper, Collections.emptySet())));
                        // @formatter:on
                    }
                }
                // if it's an include regex function
                else if (descriptor instanceof EvaluationPhaseFilterFunctionsDescriptor.EvaluationPhaseFilterJexlArgumentDescriptor) {
                    FunctionJexlNodeVisitor functionVisitor = new FunctionJexlNodeVisitor();
                    dereferenced.jjtAccept(functionVisitor, null);
                    
                    Set<String> functionFields = descriptor.fields(metadataHelper, Collections.emptySet());
                    functionFields.retainAll(mappingFields);
                    
                    // if it's 'filter:includeRegex' with a matching field
                    if (functionVisitor.namespace().equals(EVAL_PHASE_FUNCTION_NAMESPACE) && functionVisitor.name().equals(INCLUDE_REGEX)
                                    && !functionFields.isEmpty()) {
                        String includeRegex = getStringValue(functionVisitor.args().get(1));
                        
                        if (includeRegex != null) {
                            // test the regex against the desired field values to see if there's a match
                            Pattern regexPattern = Pattern.compile(includeRegex, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
                            
                            for (String fieldValue : fieldValues) {
                                if (regexPattern.matcher(fieldValue).matches()) {
                                    regexValueMatches.add(fieldValue);
                                }
                            }
                        }
                    }
                }
            }
            // get all of the value-specific, field mapping EQ nodes
            else if (dereferenced instanceof ASTEQNode) {
                String field = getStringField(dereferenced);
                String value = getStringValue(dereferenced);
                if (mappingFields.contains(field) && fieldValues.contains(value)) {
                    valueEqNodeMap.add(value, child);
                }
            }
        }
        
        if (!functionNodeInfoList.isEmpty() && (!valueEqNodeMap.isEmpty() || !regexValueMatches.isEmpty())) {
            Set<JexlNode> eqNodesToRemove = new HashSet<>();
            
            // for each GeoWave function we saw in this AND node
            for (FunctionNodeInfo nodeInfo : functionNodeInfoList) {
                List<String> newFields = new ArrayList<>();
                Set<String> fieldsToRemove = new HashSet<>();
                
                // get the full list of entries, including empty entries for the regex value matches
                List<Map.Entry<String,List<JexlNode>>> entries = new LinkedList<>(valueEqNodeMap.entrySet());
                for (String valueMatch : regexValueMatches) {
                    entries.add(new EmptyEntry(valueMatch));
                }
                
                // for each EQ node and regex value match we saw in this AND node
                for (Map.Entry<String,List<JexlNode>> entry : entries) {
                    Map<String,List<String>> fieldMappings = valueSpecificFieldMappings.get(entry.getKey());
                    
                    // if we have a field mapping for that value
                    if (fieldMappings != null && !fieldMappings.isEmpty()) {
                        
                        // for each of the fields in this GeoWave function
                        for (String field : nodeInfo.getFields()) {
                            List<String> remappedFields = fieldMappings.get(field);
                            
                            // if a field mapping exists
                            if (remappedFields != null && !remappedFields.isEmpty()) {
                                
                                // add each of the fields in the mapping to our list of new
                                // fields and mark the original field and EQ node for removal
                                newFields.addAll(remappedFields);
                                fieldsToRemove.add(field);
                                
                                if (entry.getValue() != null) {
                                    eqNodesToRemove.addAll(entry.getValue());
                                }
                            }
                        }
                    }
                }
                
                // if there are new fields, rewrite the AND node, and remove the EQ node(s)
                if (!newFields.isEmpty()) {
                    List<String> functionFields = new ArrayList<>(nodeInfo.getFields());
                    functionFields.addAll(newFields);
                    functionFields.removeAll(fieldsToRemove);
                    
                    FunctionJexlNodeVisitor functionVisitor = new FunctionJexlNodeVisitor();
                    nodeInfo.getDereferenced().jjtAccept(functionVisitor, null);
                    
                    List<ASTIdentifier> fieldIdentifiers = functionFields.stream().map(JexlNodes::makeIdentifierWithImage).collect(Collectors.toList());
                    JexlNode fieldNamesArg = (fieldIdentifiers.size() <= 1) ? fieldIdentifiers.get(0) : JexlNodeFactory.createUnwrappedOrNode(fieldIdentifiers);
                    
                    List<JexlNode> args = new ArrayList<>(functionVisitor.args());
                    args.set(0, fieldNamesArg);
                    
                    ASTFunctionNode newFunctionNode = FunctionJexlNodeVisitor.makeFunctionFrom(functionVisitor.namespace(), functionVisitor.name(),
                                    args.toArray(new JexlNode[0]));
                    
                    JexlNodes.replaceChild(nodeInfo.getDereferenced().jjtGetParent(), nodeInfo.getDereferenced(), newFunctionNode);
                }
            }
            
            // remove the EQ nodes
            children.removeAll(eqNodesToRemove);
            
            // update the children
            JexlNodes.children(node, children.toArray(new JexlNode[0]));
        }
        
        return super.visit(node, data);
    }
    
    private String getStringField(JexlNode jexlNode) {
        String field = null;
        if (jexlNode != null) {
            field = JexlASTHelper.getIdentifier(jexlNode);
        }
        return field;
    }
    
    private String getStringValue(JexlNode jexlNode) {
        String value = null;
        if (jexlNode != null) {
            try {
                Object objValue = JexlASTHelper.getLiteralValue(jexlNode);
                if (objValue != null) {
                    value = objValue.toString();
                }
            } catch (Exception e) {
                if (log.isTraceEnabled()) {
                    log.trace("Couldn't get value from jexl node: " + JexlStringBuildingVisitor.buildQuery(jexlNode));
                }
            }
        }
        return value;
    }
    
    private static class FunctionNodeInfo {
        private final JexlNode node;
        private final ASTFunctionNode dereferenced;
        private final JexlArgumentDescriptor descriptor;
        private final Set<String> fields;
        
        public FunctionNodeInfo(JexlNode node, ASTFunctionNode dereferenced, JexlArgumentDescriptor descriptor, Set<String> fields) {
            this.node = node;
            this.dereferenced = dereferenced;
            this.descriptor = descriptor;
            this.fields = fields;
        }
        
        public JexlNode getNode() {
            return node;
        }
        
        public ASTFunctionNode getDereferenced() {
            return dereferenced;
        }
        
        public JexlArgumentDescriptor getDescriptor() {
            return descriptor;
        }
        
        public Set<String> getFields() {
            return fields;
        }
    }
    
    private static class EmptyEntry implements Map.Entry<String,List<JexlNode>> {
        private final String key;
        
        public EmptyEntry(String key) {
            this.key = key;
        }
        
        @Override
        public String getKey() {
            return key;
        }
        
        @Override
        public List<JexlNode> getValue() {
            return null;
        }
        
        @Override
        public List<JexlNode> setValue(List<JexlNode> value) {
            return null;
        }
    }
}
