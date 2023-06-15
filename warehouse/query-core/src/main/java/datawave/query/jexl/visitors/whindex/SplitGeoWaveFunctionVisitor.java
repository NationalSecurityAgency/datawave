package datawave.query.jexl.visitors.whindex;

import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.GeoFunctionsDescriptor;
import datawave.query.jexl.functions.GeoWaveFunctionsDescriptor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.JexlNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This is a visitor which is used to break up geowave functions which have multiple fields into separate geowave functions.
 */
class SplitGeoWaveFunctionVisitor extends RebuildingVisitor {
    private MetadataHelper metadataHelper;
    
    private SplitGeoWaveFunctionVisitor(MetadataHelper metadataHelper) {
        this.metadataHelper = metadataHelper;
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T apply(T script, MetadataHelper metadataHelper) {
        SplitGeoWaveFunctionVisitor visitor = new SplitGeoWaveFunctionVisitor(metadataHelper);
        return (T) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        if (descriptor instanceof GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor) {
            Set<String> fields = descriptor.fields(metadataHelper, Collections.emptySet());
            if (fields.size() > 1) {
                List<JexlNode> functionNodes = new ArrayList<>();
                
                FunctionJexlNodeVisitor functionVisitor = new FunctionJexlNodeVisitor();
                node.jjtAccept(functionVisitor, null);
                
                for (String field : fields) {
                    List<JexlNode> newArgs = new ArrayList<>();
                    for (int i = 0; i < functionVisitor.args().size(); i++) {
                        if (i == 0) {
                            newArgs.add(JexlNodeFactory.buildIdentifier(field));
                        } else {
                            newArgs.add(RebuildingVisitor.copy(functionVisitor.args().get(i)));
                        }
                    }
                    
                    functionNodes.add(FunctionJexlNodeVisitor.makeFunctionFrom(functionVisitor.namespace(), functionVisitor.name(),
                                    newArgs.toArray(new JexlNode[0])));
                }
                return JexlNodeFactory.createOrNode(functionNodes);
            }
        } else if (descriptor instanceof GeoFunctionsDescriptor.GeoJexlArgumentDescriptor) {
            Set<String> fields = descriptor.fields(metadataHelper, Collections.emptySet());
            if (fields.size() > 1) {
                List<JexlNode> functionNodes = new ArrayList<>();
                
                FunctionJexlNodeVisitor functionVisitor = new FunctionJexlNodeVisitor();
                node.jjtAccept(functionVisitor, null);
                
                // geo functions with > 3 args contain separate fields for lat/lon, and should not be considered
                if (functionVisitor.args().size() == 3) {
                    for (String field : fields) {
                        List<JexlNode> newArgs = new ArrayList<>();
                        for (int i = 0; i < functionVisitor.args().size(); i++) {
                            if (i == 0) {
                                newArgs.add(JexlNodeFactory.buildIdentifier(field));
                            } else {
                                newArgs.add(RebuildingVisitor.copy(functionVisitor.args().get(i)));
                            }
                        }
                        
                        functionNodes.add(FunctionJexlNodeVisitor.makeFunctionFrom(functionVisitor.namespace(), functionVisitor.name(),
                                        newArgs.toArray(new JexlNode[0])));
                    }
                    return JexlNodeFactory.createOrNode(functionNodes);
                }
            }
        }
        return super.visit(node, data);
    }
}
