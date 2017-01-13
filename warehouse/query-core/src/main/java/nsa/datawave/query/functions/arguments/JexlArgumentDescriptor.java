package nsa.datawave.query.functions.arguments;

import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.util.Metadata;

/**
 * This interface will describe the arguments for a jexl function that has implemented (@see JexlArgumentDescriptor). The initial use of this is to determine
 * what fields and values should be queried for in the index for shard range determination.
 *
 * 
 *
 */
@Deprecated
public interface JexlArgumentDescriptor {
    /**
     * Get the nodes that can be used to determine ranges from the global index.
     *
     * @return A collection of DatawaveTreeNode
     */
    public DatawaveTreeNode getIndexQuery(Metadata metadata);
    
    /**
     * Get the argument descriptors
     */
    public JexlArgument[] getArguments();
    
    /**
     * Get the argument descriptors with field name information filled in
     */
    public JexlArgument[] getArgumentsWithFieldNames(Metadata metadata);
}
