package datawave.query.rewrite.jexl.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import datawave.data.normalizer.GeoNormalizer;
import datawave.data.normalizer.GeoNormalizer.GeoPoint;
import datawave.data.normalizer.GeoNormalizer.OutOfRangeException;
import datawave.data.normalizer.GeoNormalizer.ParseException;
import datawave.data.type.GeoType;
import datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import datawave.query.rewrite.jexl.ArithmeticJexlEngines;
import datawave.query.rewrite.jexl.JexlASTHelper;
import datawave.query.rewrite.jexl.JexlNodeFactory;
import datawave.query.rewrite.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.rewrite.jexl.functions.GeoFunctions;
import datawave.query.rewrite.jexl.functions.RefactoredJexlFunctionArgumentDescriptorFactory;
import datawave.query.rewrite.jexl.functions.arguments.RefactoredJexlArgumentDescriptor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

public class GeoFunctionsDescriptor implements RefactoredJexlFunctionArgumentDescriptorFactory {
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     */
    public static class GeoJexlArgumentDescriptor implements RefactoredJexlArgumentDescriptor {
        
        private final ASTFunctionNode node;
        private final String namespace, name;
        private final List<JexlNode> args;
        
        public GeoJexlArgumentDescriptor(ASTFunctionNode node, String namespace, String name, List<JexlNode> args) {
            this.node = node;
            this.namespace = namespace;
            this.name = name;
            this.args = args;
        }
        
        @Override
        public JexlNode getIndexQuery(RefactoredShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper,
                        Set<String> datatypeFilter) {
            // return the true node if unable to parse arguments
            JexlNode returnNode = TRUE_NODE;
            
            if (name.equals("within_bounding_box")) {
                
                // three arguments is the form within_bounding_box(fieldName, lowerLeft, upperRight)
                if (args.size() == 3) {
                    
                    JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), args.get(1).image);
                    JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), args.get(2).image);
                    
                    // now link em up
                    JexlNode andNode = JexlNodeFactory.createAndNode(Arrays.asList(new JexlNode[] {geNode, leNode}));
                    
                    returnNode = andNode;
                } else {
                    
                    JexlNode geLonNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), args.get(2).image);
                    JexlNode leLonNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), args.get(4).image);
                    
                    JexlNode geLatNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(1), args.get(3).image);
                    JexlNode leLatNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(1), args.get(5).image);
                    
                    // now link em up
                    JexlNode andNode = JexlNodeFactory.createAndNode(Arrays.asList(new JexlNode[] {geLonNode, leLonNode, geLatNode, leLatNode}));
                    
                    returnNode = andNode;
                }
            } else if (name.equals("within_circle")) {
                
                String center = args.get(1).image;
                GeoType gn = new GeoType();
                
                if (!GeoNormalizer.isNormalized(center)) {
                    try {
                        center = gn.normalize(center);
                    } catch (IllegalArgumentException ne) {
                        throw new IllegalArgumentException("Unable to parse lat_lon value: " + center);
                    }
                }
                GeoPoint c;
                try {
                    c = GeoPoint.decodeZRef(center);
                } catch (OutOfRangeException e) {
                    throw new IllegalArgumentException("Out of range center value " + center, e);
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Unparseable range center value " + center, e);
                }
                
                double radius;
                try {
                    radius = GeoNormalizer.parseDouble(args.get(2).image);
                } catch (ParseException pe) {
                    throw new IllegalArgumentException("Unable to parse radius " + args.get(2).image, pe);
                }
                double lat = c.getLatitude();
                double lon = c.getLongitude();
                
                String sep = GeoNormalizer.separator;
                JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), String.valueOf(lat + radius) + sep
                                + String.valueOf(lon + radius));
                JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), String.valueOf(lat - radius) + sep
                                + String.valueOf(lon - radius));
                
                // now link em up
                JexlNode andNode = JexlNodeFactory.createAndNode(Arrays.asList(new JexlNode[] {geNode, leNode}));
                
                returnNode = andNode;
            }
            return returnNode;
        }
        
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            if (arg > 0) {
                if (name.equals("within_bounding_box")) {
                    if (args.size() == 6) {
                        if (arg == 2 || arg == 4) {
                            return JexlASTHelper.getIdentifierNames(args.get(0));
                        } else {
                            return JexlASTHelper.getIdentifierNames(args.get(1));
                        }
                    } else {
                        return JexlASTHelper.getIdentifierNames(args.get(0));
                    }
                } else if (arg == 1) { // within_circle
                    return JexlASTHelper.getIdentifierNames(args.get(0));
                }
            }
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            if (name.equals("within_bounding_box") && args.size() == 6) {
                Set<String> fields = new HashSet<>();
                fields.addAll(JexlASTHelper.getIdentifierNames(args.get(0)));
                fields.addAll(JexlASTHelper.getIdentifierNames(args.get(1)));
                return fields;
            } else {
                return JexlASTHelper.getIdentifierNames(args.get(0));
            }
        }
        
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            if (name.equals("within_bounding_box") && args.size() == 6) {
                // if we have an or node anywhere, then we need to produce a cartesion product
                return RefactoredJexlArgumentDescriptor.Fields.product(args.get(0), args.get(1));
            } else {
                return RefactoredJexlArgumentDescriptor.Fields.product(args.get(0));
            }
        }
        
        @Override
        public boolean useOrForExpansion() {
            return true;
        }
        
        @Override
        public boolean regexArguments() {
            return false;
        }
    }
    
    @Override
    public RefactoredJexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
        fvis.visit(node, null);
        
        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(fvis.namespace());
        
        if (!GeoFunctions.GEO_FUNCTION_NAMESPACE.equals(node.jjtGetChild(0).image))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of "
                            + node.jjtGetChild(0).image);
        if (!functionClass.equals(GeoFunctions.class))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in "
                            + functionClass);
        
        verify(fvis.name(), fvis.args().size());
        
        return new GeoJexlArgumentDescriptor(node, fvis.namespace(), fvis.name(), fvis.args());
    }
    
    private static void verify(String name, int numArgs) {
        if (name.equals("within_bounding_box")) {
            // three arguments is the form within_bounding_box(fieldName, lowerLeft, upperRight)
            if (numArgs != 3 && numArgs != 6) {
                throw new IllegalArgumentException("Wrong number of arguments to within_bounding_box function");
            }
        } else if (name.equals("within_circle")) {
            if (numArgs != 3) {
                throw new IllegalArgumentException("Wrong number of arguments to within_circle function");
            }
        } else {
            throw new IllegalArgumentException("Unknown Geo function: " + name);
        }
    }
}
