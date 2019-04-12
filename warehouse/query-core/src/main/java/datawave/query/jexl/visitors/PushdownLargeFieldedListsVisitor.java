package datawave.query.jexl.visitors;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.newInstanceOfType;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.HashMultimap;
import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Logger;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.fst.FST;

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
        
        Multimap<String,JexlNode> eqNodesByField = HashMultimap.create();
        Multimap<String,JexlNode> rangeNodesByField = HashMultimap.create();
        List<JexlNode> otherNodes = new ArrayList<>();
        
        // first pull out sets of nodes by field
        for (JexlNode childNode : children(node))
            assignNodeByField(childNode, eqNodesByField, rangeNodesByField, otherNodes);
        
        ArrayList<JexlNode> children = newArrayList();
        
        // if "OTHER_NODES", then simply add the subset back into the children list
        for (JexlNode child : otherNodes) {
            JexlNode copiedChild = (JexlNode) child.jjtAccept(this, data);
            if (copiedChild != null) {
                children.add(copiedChild);
            }
        }
        
        SortedSet<String> fields = new TreeSet<>(eqNodesByField.keySet());
        fields.addAll(rangeNodesByField.keySet());
        
        for (String field : fields) {
            
            Collection<JexlNode> eqNodes = eqNodesByField.get(field);
            Collection<JexlNode> rangeNodes = rangeNodesByField.get(field);
            
            // if "_ANYFIELD_" or "_NOFIELD_", then simply add the subset back into the children list
            // if past our threshold, then add a ExceededValueThresholdMarker with an OR of this subset to the children list
            if (!Constants.ANY_FIELD.equals(field) && !Constants.NO_FIELD.equals(field)
                            && (eqNodes.size() >= config.getMaxOrExpansionThreshold() || rangeNodes.size() >= config.getMaxOrRangeThreshold())
                            && isIndexed(field)) {
                log.info("Pushing down large (" + eqNodes.size() + ") fielded list for " + field);
                
                // turn the subset of children into a list of values
                SortedSet<String> values = new TreeSet<>();
                for (JexlNode child : eqNodes) {
                    values.add(String.valueOf(JexlASTHelper.getLiteralValue(child)));
                }
                
                List<JexlNode> markers = new ArrayList<>();
                
                try {
                    // if we have an hdfs cache directory and if past the fst threshold, then create the fst and replace the list with an assignment
                    if (rangeNodesByField.isEmpty() && fstHdfsUri != null && (eqNodes.size() >= config.getMaxOrExpansionFstThreshold())) {
                        URI fstPath = createFst(values);
                        markers.add(new ExceededOrThresholdMarkerJexlNode(field, fstPath));
                        eqNodes = null;
                    } else if (eqNodes.size() >= config.getMaxOrExpansionThreshold()) {
                        markers.add(new ExceededOrThresholdMarkerJexlNode(field, values, null));
                        eqNodes = null;
                    }
                    
                    // handle range nodes separately
                    if (rangeNodes.size() >= config.getMaxOrRangeThreshold()) {
                        Collection<Range> ranges = new TreeSet<>();
                        rangeNodes.forEach(rangeNode -> ranges.add(rangeNodeToRange(rangeNode)));
                        
                        markers.add(new ExceededOrThresholdMarkerJexlNode(field, null, ranges));
                        rangeNodes = null;
                    }
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException e) {
                    QueryException qe = new QueryException(DatawaveErrorCode.LARGE_FIELDED_LIST_ERROR, e);
                    throw new DatawaveFatalQueryException(qe);
                }
                
                // add in any unused eq nodes
                if (eqNodes != null) {
                    for (JexlNode child : eqNodes) {
                        JexlNode copiedChild = (JexlNode) child.jjtAccept(this, data);
                        if (copiedChild != null) {
                            children.add(copiedChild);
                        }
                    }
                }
                
                // add in any unused range nodes
                if (rangeNodes != null) {
                    for (JexlNode child : rangeNodes) {
                        JexlNode copiedChild = (JexlNode) child.jjtAccept(this, data);
                        if (copiedChild != null) {
                            children.add(copiedChild);
                        }
                    }
                }
                
                children.addAll(markers);
            }
            // else simply add the subset back into the children list
            else {
                // recurse on the eq children in this subset
                for (JexlNode child : eqNodes) {
                    JexlNode copiedChild = (JexlNode) child.jjtAccept(this, data);
                    if (copiedChild != null) {
                        children.add(copiedChild);
                    }
                }
                
                // recurse on the range children in this subset
                for (JexlNode child : rangeNodes) {
                    JexlNode copiedChild = (JexlNode) child.jjtAccept(this, data);
                    if (copiedChild != null) {
                        children.add(copiedChild);
                    }
                }
            }
        }
        
        return children(newNode, children.toArray(new JexlNode[children.size()]));
    }
    
    protected Range rangeNodeToRange(JexlNode node) {
        if (ExceededValueThresholdMarkerJexlNode.instanceOf(node)) {
            return rangeNodeToRange(ExceededValueThresholdMarkerJexlNode.getExceededValueThresholdSource(node));
        } else if ((node.jjtGetNumChildren() == 1) && (node instanceof ASTReferenceExpression || node instanceof ASTReference || node instanceof ASTAndNode)) {
            return rangeNodeToRange(node.jjtGetChild(0));
        } else if ((node.jjtGetNumChildren() == 2) && node instanceof ASTAndNode) {
            JexlNode leftChild = node.jjtGetChild(0);
            JexlNode rightChild = node.jjtGetChild(1);
            return new Range(new Key(String.valueOf(JexlASTHelper.getLiteralValue(leftChild))), leftChild instanceof ASTGENode, new Key(
                            String.valueOf(JexlASTHelper.getLiteralValue(rightChild))), rightChild instanceof ASTLENode);
        } else {
            return null;
        }
    }
    
    protected boolean isIndexed(String field) {
        return config.getIndexedFields().contains(JexlASTHelper.deconstructIdentifier(field));
    }
    
    protected void assignNodeByField(JexlNode origNode, Multimap<String,JexlNode> eqNodes, Multimap<String,JexlNode> rangeNodes, List<JexlNode> otherNodes) {
        assignNodeByField(origNode, origNode, eqNodes, rangeNodes, otherNodes);
    }
    
    protected void assignNodeByField(JexlNode origNode, JexlNode subNode, Multimap<String,JexlNode> eqNodes, Multimap<String,JexlNode> rangeNodes,
                    List<JexlNode> otherNodes) {
        if (subNode instanceof ASTEQNode) {
            eqNodes.put(JexlASTHelper.getIdentifier(subNode), origNode);
        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(subNode)) {
            assignNodeByField(origNode, ExceededValueThresholdMarkerJexlNode.getExceededValueThresholdSource(subNode), eqNodes, rangeNodes, otherNodes);
        }
        // else if (ASTDelayedPredicate.instanceOf(subNode)) {
        // assignNodeByField(origNode, ASTDelayedPredicate.getDelayedPredicateSource(subNode), eqNodes, rangeNodes, otherNodes);
        // }
        else if ((subNode.jjtGetNumChildren() == 1)
                        && (subNode instanceof ASTReferenceExpression || subNode instanceof ASTReference || subNode instanceof ASTAndNode)) {
            assignNodeByField(origNode, subNode.jjtGetChild(0), eqNodes, rangeNodes, otherNodes);
        } else if ((subNode.jjtGetNumChildren() == 2) && subNode instanceof ASTAndNode) {
            JexlNode leftChild = subNode.jjtGetChild(0);
            JexlNode rightChild = subNode.jjtGetChild(1);
            if ((leftChild instanceof ASTGTNode || leftChild instanceof ASTGENode) && (rightChild instanceof ASTLTNode || rightChild instanceof ASTLENode)) {
                String leftField = JexlASTHelper.getIdentifier(leftChild);
                String rightField = JexlASTHelper.getIdentifier(rightChild);
                if (leftField != null && rightField != null && leftField.equals(rightField)) {
                    rangeNodes.put(leftField, origNode);
                } else {
                    otherNodes.add(origNode);
                }
            } else {
                otherNodes.add(origNode);
            }
        } else {
            otherNodes.add(origNode);
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
