package datawave.query.jexl.visitors;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.newInstanceOfType;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Logger;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.fst.FST;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Visits a JexlNode tree, and take large (defined by config.getFieldedListThreshold) lists of values against a single field into an FST ivarator (bypass the
 * global index and scan the field indexes instead)
 *
 */
public class PushdownLargeFieldedListsVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(PushdownLargeFieldedListsVisitor.class);
    private ShardQueryConfiguration config;
    private String fstHdfsUri;
    private FileSystem fs;
    
    public PushdownLargeFieldedListsVisitor(ShardQueryConfiguration config, FileSystem fs, String fstHdfsUri) {
        this.config = config;
        this.fstHdfsUri = fstHdfsUri;
        this.fs = fs;
    }
    
    /**
     * Expand functions to be AND'ed with their index query equivalents.
     *
     * @param script
     * @return The tree with additional index query portions
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T pushdown(ShardQueryConfiguration config, T script, FileSystem fs, String fstHdfsUri) {
        // flatten the tree
        script = TreeFlatteningRebuildingVisitor.flatten(script);
        
        PushdownLargeFieldedListsVisitor visitor = new PushdownLargeFieldedListsVisitor(config, fs, fstHdfsUri);
        
        return (T) script.jjtAccept(visitor, null);
    }
    
    // OTHER_NODES sorts before all other field names
    private static final String OTHER_NODES = "!" + PushdownLargeFieldedListsVisitor.class.getName() + ".otherNodes";
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        ASTOrNode newNode = newInstanceOfType(node);
        newNode.image = node.image;
        
        // first pull out sets of nodes by field
        Multimap<String,JexlNode> nodes = getNodesByField(children(node));
        
        ArrayList<JexlNode> children = newArrayList();
        List<String> fields = new ArrayList<String>(nodes.keySet());
        Collections.sort(fields);
        for (String field : fields) {
            // recurse on the children in this subset
            Collection<JexlNode> subsetChildren = nodes.get(field);
            List<JexlNode> subsetChildrenCopies = new ArrayList<JexlNode>();
            for (JexlNode child : subsetChildren) {
                JexlNode copiedChild = (JexlNode) child.jjtAccept(this, data);
                if (copiedChild != null) {
                    subsetChildrenCopies.add(copiedChild);
                }
            }
            
            // if "OTHER_NODES", then simply add the subset back into the children list
            // or if "_ANYFIELD_" , then simply add the subset back into the children list
            if (OTHER_NODES.equals(field) || Constants.ANY_FIELD.equals(field)) {
                children.addAll(subsetChildrenCopies);
            }
            // if past our threshold, then add a ExceededValueThresholdMarker with an OR of this subset to the children list
            else if (subsetChildrenCopies.size() >= config.getMaxOrExpansionThreshold()) {
                log.info("Pushing down large (" + subsetChildrenCopies.size() + ") fielded list for " + field);
                
                // turn the subset of children into a list of values
                SortedSet<String> values = new TreeSet<String>();
                for (JexlNode child : subsetChildrenCopies) {
                    values.add(String.valueOf(JexlASTHelper.getLiteralValue(child)));
                }
                
                ExceededOrThresholdMarkerJexlNode marker = null;
                
                // if we have an hdfs cache directory and if past the fst threshold, then create the fst and replace the list with an assignment
                if (fstHdfsUri != null && (subsetChildrenCopies.size() >= config.getMaxOrExpansionFstThreshold())) {
                    URI fstPath;
                    try {
                        fstPath = createFst(values);
                    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException e) {
                        QueryException qe = new QueryException(DatawaveErrorCode.FST_CREATE_ERROR, e);
                        throw new DatawaveFatalQueryException(qe);
                    }
                    
                    marker = new ExceededOrThresholdMarkerJexlNode(field, fstPath);
                } else {
                    marker = new ExceededOrThresholdMarkerJexlNode(field, values);
                }
                children.add(marker);
            }
            // else simply add the subset back into the children list
            else {
                children.addAll(subsetChildrenCopies);
            }
        }
        
        return children(newNode, children.toArray(new JexlNode[children.size()]));
    }
    
    /**
     * Get the nodes mapped by fieldname. Only equality and negated equalities are included. Anything else is placed into the OTHER bucket.
     * 
     * @param nodes
     * @return the nodes mapped by filename
     */
    protected Multimap<String,JexlNode> getNodesByField(JexlNode[] nodes) {
        Multimap<String,JexlNode> nodeMap = ArrayListMultimap.create();
        for (JexlNode node : nodes) {
            nodeMap.put(getEqualityIdentifier(node), node);
        }
        return nodeMap;
    }
    
    /**
     * Get the identifier from the equality. If this node is anything else then OTHER_NODES is returned.
     * 
     * @param node
     * @return the node's identifier iff EQ node, otherwise OTHER_NODES.
     */
    protected String getEqualityIdentifier(JexlNode node) {
        if (node instanceof ASTEQNode) {
            return JexlASTHelper.getIdentifier(node);
        } else if ((node.jjtGetNumChildren() == 1) && (node instanceof ASTReferenceExpression || node instanceof ASTReference)) {
            return getEqualityIdentifier(node.jjtGetChild(0));
        } else {
            return OTHER_NODES;
        }
    }
    
    protected URI createFst(SortedSet<String> values) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        FST fst = DatawaveFieldIndexListIteratorJexl.getFST(values);
        
        // now serialize to our file system
        CompressionCodec codec = null;
        String extension = "";
        if (config.getHdfsFileCompressionCodec() != null) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = this.getClass().getClassLoader();
            }
            Class<? extends CompressionCodec> clazz = Class.forName(config.getHdfsFileCompressionCodec(), true, classLoader).asSubclass(CompressionCodec.class);
            codec = clazz.newInstance();
            extension = codec.getDefaultExtension();
        }
        int fstCount = config.getFstCount().incrementAndGet();
        Path fstFile = new Path(fstHdfsUri, "PushdownLargeFileFst." + fstCount + ".fst" + extension);
        
        OutputStream fstFileOut = new BufferedOutputStream(fs.create(fstFile, false));
        if (codec != null) {
            fstFileOut = codec.createOutputStream(fstFileOut);
        }
        
        OutputStreamDataOutput outStream = new OutputStreamDataOutput(fstFileOut);
        fst.save(outStream);
        outStream.close();
        
        return fstFile.toUri();
    }
    
}
