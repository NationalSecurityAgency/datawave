package nsa.datawave.query.rewrite.jexl.functions;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import nsa.datawave.data.normalizer.GeometryNormalizer;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.jexl.ArithmeticJexlEngines;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.JexlNodeFactory;
import nsa.datawave.query.rewrite.jexl.functions.arguments.RefactoredJexlArgumentDescriptor;
import nsa.datawave.query.util.DateIndexHelper;
import nsa.datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This is the descriptor class for performing geowave functions. Its supprts basic spatial relationships, and decomposes the bounding box of the relationship
 * geometry into a set of geowave ranges. It currently caps this range decomposition to 800 ranges and in practice should be considerably less.
 */
public class GeoWaveFunctionsDescriptor implements RefactoredJexlFunctionArgumentDescriptorFactory {
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     */
    protected static final String[] SPATIAL_RELATION_OPERATIONS = new String[] {"contains", "covers", "covered_by", "crosses", "intersects", "overlaps",
            "within"};
    protected static final int MAX_EXPANSION = 800;
    private static final Logger LOGGER = Logger.getLogger(GeoWaveFunctionsDescriptor.class);
    
    public static class GeoWaveJexlArgumentDescriptor implements RefactoredJexlArgumentDescriptor {
        protected final String name;
        protected final List<JexlNode> args;
        
        public GeoWaveJexlArgumentDescriptor(ASTFunctionNode node, String name, List<JexlNode> args) {
            this.name = name;
            this.args = args;
        }
        
        @Override
        public JexlNode getIndexQuery(RefactoredShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper,
                        Set<String> datatypeFilter) {
            if (isSpatialRelationship(name)) {
                Geometry geom = GeometryNormalizer.getGeometryFromWKT(args.get(1).image);
                return getIndexNode(args.get(0), geom.getEnvelopeInternal());
            }
            // return the true node if unable to parse arguments
            return TRUE_NODE;
        }
        
        protected static JexlNode getIndexNode(JexlNode node, Envelope env) {
            if (node.jjtGetNumChildren() > 0) {
                List<JexlNode> list = Lists.newArrayList();
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode kid = node.jjtGetChild(i);
                    if (kid.image != null) {
                        list.add(getIndexNode(kid.image, env));
                    }
                }
                if (list.size() > 0) {
                    return JexlNodeFactory.createOrNode(list);
                }
            } else if (node.image != null) {
                return getIndexNode(node.image, env);
            }
            return node;
        }
        
        protected static JexlNode getIndexNode(JexlNode node, List<Envelope> envs) {
            if (node.jjtGetNumChildren() > 0) {
                List<JexlNode> list = Lists.newArrayList();
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode kid = node.jjtGetChild(i);
                    if (kid.image != null) {
                        list.add(getIndexNode(kid.image, envs));
                    }
                }
                if (list.size() > 0) {
                    return JexlNodeFactory.createOrNode(list);
                }
            } else if (node.image != null) {
                return getIndexNode(node.image, envs);
            }
            return node;
        }
        
        protected static JexlNode getIndexNode(String fieldName, Envelope env) {
            List<Envelope> envs = new ArrayList<Envelope>();
            envs.add(env);
            return getIndexNode(fieldName, envs);
        }
        
        protected static JexlNode getIndexNode(String fieldName, List<Envelope> envs) {
            List<ByteArrayRange> allRanges = new ArrayList<ByteArrayRange>();
            int maxRanges = MAX_EXPANSION / envs.size();
            for (Envelope env : envs) {
                for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromEnvelope(env).getIndexConstraints(GeometryNormalizer.indexStrategy)) {
                    allRanges.addAll(GeometryNormalizer.indexStrategy.getQueryRanges(range, maxRanges));
                }
            }
            Iterable<JexlNode> rangeNodes = Iterables.transform(allRanges, new ByteArrayRangeToJexlNode(fieldName));
            
            // now link em up
            return JexlNodeFactory.createOrNode(rangeNodes);
        }
        
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // no normalization required
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            return JexlASTHelper.getIdentifierNames(args.get(0));
        }
        
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            return Fields.product(args.get(0));
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
        
        if (!GeoWaveFunctions.GEOWAVE_FUNCTION_NAMESPACE.equals(node.jjtGetChild(0).image))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of "
                            + node.jjtGetChild(0).image);
        if (!functionClass.equals(GeoWaveFunctions.class))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in "
                            + functionClass);
        
        verify(fvis.name(), fvis.args().size());
        
        return new GeoWaveJexlArgumentDescriptor(node, fvis.name(), fvis.args());
    }
    
    protected static void verify(String name, int numArgs) {
        if (isSpatialRelationship(name)) {
            // two arguments in the form <spatial_relation_function>(fieldName,
            // geometryWellKnownText
            verify(name, numArgs, new String[] {"fieldName", "geometryWellKnownText"});
        } else {
            throw new IllegalArgumentException("Unknown GeoWave function: " + name);
        }
    }
    
    private static boolean isSpatialRelationship(String name) {
        for (String op : SPATIAL_RELATION_OPERATIONS) {
            if (op.equals(name)) {
                return true;
            }
        }
        return false;
    }
    
    protected static void verify(String name, int numArgs, String[] parameterNames) {
        if (numArgs != parameterNames.length) {
            StringBuilder exception = new StringBuilder("Wrong number of arguments to GeoWave's ");
            exception.append(name).append(" function; correct use is '").append(name).append("(");
            if (parameterNames.length > 0) {
                exception.append(parameterNames[0]);
            }
            for (int i = 1; i < parameterNames.length; i++) {
                exception.append(", ").append(parameterNames[i]);
            }
            exception.append(")'");
            throw new IllegalArgumentException(exception.toString());
        }
    }
    
    private static class ByteArrayRangeToJexlNode implements com.google.common.base.Function<ByteArrayRange,JexlNode> {
        private String fieldName;
        
        public ByteArrayRangeToJexlNode(String fieldName) {
            this.fieldName = fieldName;
        }
        
        @Override
        public JexlNode apply(ByteArrayRange input) {
            if (input.getStart().equals(input.getEnd())) {
                return JexlNodeFactory.buildNode(new ASTEQNode(ParserTreeConstants.JJTEQNODE), fieldName,
                                GeometryNormalizer.getEncodedStringFromIndexBytes(input.getStart()));
            } else {
                JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), fieldName,
                                GeometryNormalizer.getEncodedStringFromIndexBytes(input.getStart()));
                JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), fieldName,
                                GeometryNormalizer.getEncodedStringFromIndexBytes(input.getEnd()));
                // now link em up
                return JexlNodeFactory.createAndNode(Arrays.asList(new JexlNode[] {geNode, leNode}));
            }
        }
        
    }
    
}
