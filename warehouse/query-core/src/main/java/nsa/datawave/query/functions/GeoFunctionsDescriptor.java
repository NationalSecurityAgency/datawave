package nsa.datawave.query.functions;

import java.util.List;

import nsa.datawave.data.normalizer.GeoNormalizer;
import nsa.datawave.data.normalizer.GeoNormalizer.ParseException;
import nsa.datawave.query.functions.arguments.JexlArgument;
import nsa.datawave.query.functions.arguments.JexlArgumentDescriptor;
import nsa.datawave.query.functions.arguments.JexlFieldNameArgument;
import nsa.datawave.query.functions.arguments.JexlValueArgument;
import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.util.Metadata;

import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

@Deprecated
public class GeoFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    private static final Logger log = Logger.getLogger(GeoFunctionsDescriptor.class);
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     * 
     *
     */
    public static class GeoJexlArgumentDescriptor implements JexlArgumentDescriptor {
        
        private DatawaveTreeNode node = null;
        
        public GeoJexlArgumentDescriptor(DatawaveTreeNode node) {
            this.node = node;
            init();
        }
        
        private void init() {
            List<JexlNode> args = node.getFunctionArgs();
            if (args.size() == 0) {
                log.error("Missing arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                throw new IllegalArgumentException("Missing arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
            }
        }
        
        @Override
        public DatawaveTreeNode getIndexQuery(Metadata metadata) {
            DatawaveTreeNode root = null;
            List<JexlNode> args = node.getFunctionArgs();
            if (args.size() != 3 && args.size() != 6) {
                log.error("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
            }
            // at this point we expect the argument to be the field name
            if (node.getFunctionName().equals("within_bounding_box")) {
                if (args.size() == 3) {
                    root = new DatawaveTreeNode(ParserTreeConstants.JJTLENODE);
                    root.setRangeNode(true);
                    root.setFieldName(args.get(0).image);
                    root.setLowerBound(args.get(1).image);
                    root.setRangeLowerOp(">=");
                    root.setUpperBound(args.get(2).image);
                    root.setRangeUpperOp("<=");
                } else {
                    root = new DatawaveTreeNode(ParserTreeConstants.JJTANDNODE);
                    
                    DatawaveTreeNode latitude = new DatawaveTreeNode(ParserTreeConstants.JJTLENODE);
                    latitude.setRangeNode(true);
                    latitude.setFieldName(args.get(0).image);
                    latitude.setLowerBound(args.get(2).image);
                    latitude.setRangeLowerOp(">=");
                    latitude.setUpperBound(args.get(3).image);
                    latitude.setRangeUpperOp("<=");
                    root.add(latitude);
                    
                    DatawaveTreeNode longitude = new DatawaveTreeNode(ParserTreeConstants.JJTLENODE);
                    longitude.setRangeNode(true);
                    longitude.setFieldName(args.get(1).image);
                    longitude.setLowerBound(args.get(4).image);
                    longitude.setRangeLowerOp(">=");
                    longitude.setUpperBound(args.get(5).image);
                    longitude.setRangeUpperOp("<=");
                    root.add(longitude);
                }
            } else if (node.getFunctionName().equals("within_circle")) {
                try {
                    double radius = GeoNormalizer.parseDouble(args.get(2).image);
                    String latlon = args.get(1).image;
                    int split = new GeoNormalizer().findSplit(latlon);
                    if (split > 0) {
                        char sep = latlon.charAt(split);
                        double lat = GeoNormalizer.parseDouble(latlon.substring(0, split));
                        double lon = GeoNormalizer.parseDouble(latlon.substring(split + 1));
                        // TODO: Detemine what to do when this falls off the edge of the world (i.e. GeoPoint.validate fails)
                        root = new DatawaveTreeNode(ParserTreeConstants.JJTLENODE);
                        root.setRangeNode(true);
                        root.setFieldName(args.get(0).image);
                        root.setLowerBound(String.valueOf(lat - radius) + sep + String.valueOf(lon - radius));
                        root.setRangeLowerOp(">=");
                        root.setUpperBound(String.valueOf(lat + radius) + sep + String.valueOf(lon + radius));
                        root.setRangeUpperOp("<=");
                    }
                } catch (ParseException pe) {
                    log.error("Unable to parse function arguments", pe);
                    throw new IllegalArgumentException("Unable to parse function arguments", pe);
                }
            }
            if (root != null) {
                root.setNegated(node.isNegated());
            }
            return root;
        }
        
        @Override
        public JexlArgument[] getArguments() {
            return getArgumentsWithFieldNames(null);
        }
        
        @Override
        public JexlArgument[] getArgumentsWithFieldNames(Metadata metadata) {
            List<JexlNode> nodes = node.getFunctionArgs();
            if (nodes.size() != 3 && nodes.size() != 6) {
                log.error("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
            }
            JexlArgument[] args = new JexlArgument[nodes.size()];
            args[0] = new JexlFieldNameArgument(nodes.get(0));
            if (node.getFunctionName().equals("within_bounding_box")) {
                if (args.length == 3) {
                    args[1] = new JexlValueArgument("lower left", nodes.get(1), nodes.get(0));
                    args[2] = new JexlValueArgument("upper right", nodes.get(2), nodes.get(0));
                } else {
                    // args[0] is already the field name argument for latitude
                    args[1] = new JexlFieldNameArgument(nodes.get(1));
                    args[2] = new JexlValueArgument("lat min bound", nodes.get(2), nodes.get(0));
                    args[3] = new JexlValueArgument("lat max bound", nodes.get(3), nodes.get(0));
                    args[4] = new JexlValueArgument("lon min bound", nodes.get(4), nodes.get(1));
                    args[5] = new JexlValueArgument("lon max bound", nodes.get(5), nodes.get(1));
                }
            } else {
                args[1] = new JexlValueArgument("center", nodes.get(1), nodes.get(0));
                args[2] = new JexlValueArgument("radius", nodes.get(2), nodes.get(0));
            }
            return args;
        }
    }
    
    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(DatawaveTreeNode node) {
        if (!node.getFunctionClass().equals(GeoFunctions.class)) {
            log.error("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with tree node for a function in "
                            + node.getFunctionClass().getSimpleName());
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with tree node for a function in "
                            + node.getFunctionClass().getSimpleName());
        }
        return new GeoJexlArgumentDescriptor(node);
    }
    
}
