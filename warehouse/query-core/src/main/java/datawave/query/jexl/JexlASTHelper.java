package datawave.query.jexl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.lookup.RangeStream;
import datawave.query.index.stats.IndexStatsClient;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;

public class JexlASTHelper extends datawave.core.query.jexl.JexlASTHelper {
    private JexlASTHelper() {
        super();
    }

    public static Set<String> getFieldNames(ASTFunctionNode function, MetadataHelper metadata, Set<String> datatypeFilter) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(function);

        return desc.fields(metadata, datatypeFilter);
    }

    public static Set<Set<String>> getFieldNameSets(ASTFunctionNode function, MetadataHelper metadata, Set<String> datatypeFilter) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(function);

        return desc.fieldSets(metadata, datatypeFilter);
    }

    /**
     * Determine if the given ASTEQNode is indexed based off of the Multimap of String fieldname to TextNormalizer.
     *
     * @param node
     *            a JexlNode
     * @param config
     *            query configuration
     * @return if it is indexed
     */
    public static boolean isIndexed(JexlNode node, ShardQueryConfiguration config) {
        Preconditions.checkNotNull(config);

        final Multimap<String,Type<?>> indexedFieldsDatatypes = config.getQueryFieldsDatatypes();

        Preconditions.checkNotNull(indexedFieldsDatatypes);

        // We expect the node to be `field op value` here
        final Collection<ASTIdentifier> identifiers = datawave.core.query.jexl.JexlASTHelper.getIdentifiers(node);
        if (1 != identifiers.size()) {
            return false;
        }

        // Clean the image off of the ASTIdentifier
        final String fieldName = deconstructIdentifier(identifiers.iterator().next());

        // Determine if the field name has associated dataTypes (is indexed)
        return RangeStream.isIndexed(fieldName, indexedFieldsDatatypes);
    }

    /**
     * Return the selectivity of the node's identifier, or IndexStatsClient.DEFAULT_VALUE if there's an error getting the selectivity
     *
     * @param node
     *            a JexlNode
     * @param config
     *            query configuration
     * @param stats
     *            index stats client
     * @return the selectivity of the node's identifier
     */
    public static Double getNodeSelectivity(JexlNode node, ShardQueryConfiguration config, IndexStatsClient stats) {
        List<ASTIdentifier> idents = getIdentifiers(node);

        // If there isn't one identifier you don't need to check the selectivity
        if (idents.size() != 1) {
            return IndexStatsClient.DEFAULT_VALUE;
        }

        return getNodeSelectivity(Sets.newHashSet(datawave.core.query.jexl.JexlASTHelper.deconstructIdentifier(idents.get(0))), config, stats);
    }

    /**
     * Return the selectivity of the node's identifier, or IndexStatsClient.DEFAULT_VALUE if there's an error getting the selectivity
     *
     * @param fieldNames
     *            Set of field names
     * @param config
     *            shard query configuration
     * @param stats
     *            the IndexStatsClient
     * @return the selectivity of the node's identifier
     */
    public static Double getNodeSelectivity(Set<String> fieldNames, ShardQueryConfiguration config, IndexStatsClient stats) {

        boolean foundSelectivity = false;

        Double maxSelectivity = Double.valueOf("-1");
        if (null != config.getIndexStatsTableName()) {
            Map<String,Double> stat = stats.safeGetStat(fieldNames, config.getDatatypeFilter(), config.getBeginDate(), config.getEndDate());
            for (Map.Entry<String,Double> entry : stat.entrySet()) {
                Double val = entry.getValue();
                // Should only get DEFAULT_STRING and DEFAULT_VALUE if there was some sort of issue getting the stats,
                // so skip this entry
                if (entry.getKey().equals(IndexStatsClient.DEFAULT_STRING) && val.equals(IndexStatsClient.DEFAULT_VALUE)) {
                    // do nothin
                } else if (val > maxSelectivity) {
                    maxSelectivity = val;
                    foundSelectivity = true;
                }
            }
        }
        // No selectivities were found, so return the default selectivity
        // from the IndexStatsClient
        if (!foundSelectivity) {

            return IndexStatsClient.DEFAULT_VALUE;
        }

        return maxSelectivity;
    }
}
