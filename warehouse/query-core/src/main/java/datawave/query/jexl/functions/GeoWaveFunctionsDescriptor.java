package datawave.query.jexl.functions;

import datawave.core.geo.function.AbstractGeoFunctionDetails;
import datawave.core.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.data.type.Type;
import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.functions.arguments.GeoFunctionJexlArgumentDescriptor;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is the descriptor class for performing geowave functions. It supports basic spatial relationships, and decomposes the bounding box of the relationship
 * geometry into a set of geowave ranges. It currently caps this range decomposition to 8 ranges per tier for GeometryType and 32 ranges total for PointType.
 *
 */
public class GeoWaveFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {

    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     */

    private static final Logger LOGGER = Logger.getLogger(GeoWaveFunctionsDescriptor.class);

    public static class GeoWaveJexlArgumentDescriptor implements JexlArgumentDescriptor, GeoFunctionJexlArgumentDescriptor {
        protected final String name;
        protected final List<JexlNode> args;
        protected final AbstractGeoFunctionDetails geoFunction;

        public GeoWaveJexlArgumentDescriptor(ASTFunctionNode node, String name, List<JexlNode> args, AbstractGeoFunctionDetails geoFunction) {
            this.name = name;
            this.args = args;
            this.geoFunction = geoFunction;
        }

        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            try {
                Map<String,Set<Type<?>>> typesByField = new HashMap<>();
                for (String field : geoFunction.getFields()) {
                    typesByField.put(field, helper.getDatatypesForField(field, datatypeFilter));
                }

                return geoFunction.generateIndexNode(typesByField, config.getGeoQueryConfig());
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to create index node for geowave function.", e);
            }
        }

        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }

        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // no normalization required
            return Collections.emptySet();
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            Set<String> fields = Collections.emptySet();
            if (geoFunction != null) {
                fields = geoFunction.getFields();
            }
            return fields;
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

        @Override
        public boolean allowIvaratorFiltering() {
            return false;
        }

        @Override
        public AbstractGeoFunctionDetails getGeoFunction() {
            return geoFunction;
        }
    }

    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
        fvis.visit(node, null);

        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(fvis.namespace());

        if (!GeoWaveFunctions.GEOWAVE_FUNCTION_NAMESPACE.equals(fvis.namespace()))
            throw new IllegalArgumentException(
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of " + fvis.namespace());
        if (!functionClass.equals(GeoWaveFunctions.class))
            throw new IllegalArgumentException(
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in " + functionClass);

        AbstractGeoFunctionDetails geoFunction = datawave.core.geo.utils.GeoWaveUtils.parseGeoWaveFunction(fvis.name(), fvis.args());
        if (geoFunction == null) {
            throw new IllegalArgumentException("Unable to parse geowave function from " + JexlStringBuildingVisitor.buildQuery(node));
        }

        return new GeoWaveJexlArgumentDescriptor(node, fvis.name(), fvis.args(), geoFunction);
    }
}
