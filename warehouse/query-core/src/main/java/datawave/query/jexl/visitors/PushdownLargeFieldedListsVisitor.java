package datawave.query.jexl.visitors;

import static com.google.common.collect.Lists.newArrayList;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static org.apache.commons.jexl3.parser.JexlNodes.newInstanceOfType;
import static org.apache.commons.jexl3.parser.JexlNodes.setChildren;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Logger;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.fst.FST;

import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

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
    private Set<String> fields;

    public PushdownLargeFieldedListsVisitor(ShardQueryConfiguration config, FileSystem fs, String fstHdfsUri, Set<String> fields) {
        this.config = config;
        this.fstHdfsUri = fstHdfsUri;
        this.fs = fs;
        this.fields = fields;
    }

    /**
     * Expand functions to be AND'ed with their index query equivalents.
     *
     * @param script
     *            a script
     * @param config
     *            a config
     * @param <T>
     *            type of script
     * @param fs
     *            filesystem
     * @param fstHdfsUri
     *            the filesystem hdfs uri
     * @return The tree with additional index query portions
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T pushdown(ShardQueryConfiguration config, T script, FileSystem fs, String fstHdfsUri) {
        return pushdown(config, script, fs, fstHdfsUri, null, null);
    }

    public static <T extends JexlNode> T pushdown(ShardQueryConfiguration config, T script, FileSystem fs, String fstHdfsUri,
                    Map<String,Integer> pushdownCapacity) {
        return pushdown(config, script, fs, fstHdfsUri, pushdownCapacity, null);
    }

    public static <T extends JexlNode> T pushdown(ShardQueryConfiguration config, T script, FileSystem fs, String fstHdfsUri, Object data, Set<String> fields) {
        // flatten the tree
        script = TreeFlatteningRebuildingVisitor.flatten(script);

        PushdownLargeFieldedListsVisitor visitor = new PushdownLargeFieldedListsVisitor(config, fs, fstHdfsUri, fields);

        return (T) script.jjtAccept(visitor, data);
    }

    // OTHER_NODES sorts before all other field names
    private static final String OTHER_NODES = "!" + PushdownLargeFieldedListsVisitor.class.getName() + ".otherNodes";

    @Override
    public Object visit(ASTOrNode node, Object data) {
        ASTOrNode newNode = newInstanceOfType(node);
        JexlNodes.copyImage(node, newNode);
        Multimap<String,JexlNode> eqNodesByField = LinkedListMultimap.create();
        Multimap<String,JexlNode> rangeNodesByField = LinkedListMultimap.create();
        List<JexlNode> otherNodes = new ArrayList<>();

        // first pull out sets of nodes by field
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            assignNodeByField(node.jjtGetChild(i), eqNodesByField, rangeNodesByField, otherNodes);
        }

        ArrayList<JexlNode> children = newArrayList();

        // if "OTHER_NODES", then simply add the subset back into the children list
        copyChildren(otherNodes, children, data);

        SortedSet<String> fields = new TreeSet<>(eqNodesByField.keySet());
        fields.addAll(rangeNodesByField.keySet());

        for (String field : fields) {
            // if fields is not specified or the current field is in fields it can be reduced
            boolean canReduce = (this.fields == null || this.fields.contains(field));

            Collection<JexlNode> eqNodes = eqNodesByField.get(field);
            Collection<JexlNode> rangeNodes = rangeNodesByField.get(field);

            // if "_ANYFIELD_" or "_NOFIELD_", then simply add the subset back into the children list
            // if past our threshold, then add a ExceededValueThresholdMarker with an OR of this subset to the children list
            // @formatter:off
            if (canReduce &&
                    !Constants.ANY_FIELD.equals(field) &&
                    !Constants.NO_FIELD.equals(field) &&
                    (eqNodes.size() >= config.getMaxOrExpansionFstThreshold() ||
                            eqNodes.size() >= config.getMaxOrExpansionThreshold() ||
                            rangeNodes.size() >= config.getMaxOrRangeThreshold()
                    ) &&
                    isIndexed(field)) {
                // @formatter:on

                log.info("Pushing down large (" + eqNodes.size() + "|" + rangeNodes.size() + ") fielded list for " + field);

                // turn the subset of children into a list of values
                SortedSet<String> values = new TreeSet<>();
                for (JexlNode child : eqNodes) {
                    values.add(String.valueOf(JexlASTHelper.getLiteralValue(child)));
                }

                List<JexlNode> markers = new ArrayList<>();

                try {
                    // if we have an hdfs cache directory and if past the fst/list threshold, then create the fst/list and replace the list with an assignment
                    if (fstHdfsUri != null && (eqNodes.size() >= config.getMaxOrExpansionFstThreshold())) {
                        URI fstPath = createFst(values);
                        markers.add(QueryPropertyMarker.create(new ExceededOr(field, fstPath).getJexlNode(), EXCEEDED_OR));
                        eqNodes = null;
                    } else if (eqNodes.size() >= config.getMaxOrExpansionThreshold()) {
                        markers.add(QueryPropertyMarker.create(new ExceededOr(field, values).getJexlNode(), EXCEEDED_OR));
                        eqNodes = null;
                    }

                    // handle range nodes separately
                    if (rangeNodes.size() >= config.getMaxOrRangeThreshold()) {
                        TreeMap<Range,JexlNode> ranges = new TreeMap<>();
                        rangeNodes.forEach(rangeNode -> ranges.put(rangeNodeToRange(rangeNode), rangeNode));

                        int numBatches = getBatchCount(rangeNodes.size());
                        List<List<Map.Entry<Range,JexlNode>>> batchedRanges = batchRanges(ranges, numBatches);

                        rangeNodes = new ArrayList<>();
                        for (List<Map.Entry<Range,JexlNode>> rangeList : batchedRanges) {
                            if (rangeList.size() > 1) {
                                markers.add(QueryPropertyMarker.create(
                                                new ExceededOr(field, rangeList.stream().map(Map.Entry::getKey).collect(Collectors.toList())).getJexlNode(),
                                                EXCEEDED_OR));
                            } else {
                                rangeNodes.add(rangeList.get(0).getValue());
                            }
                        }
                    }
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException | NoSuchMethodException
                                | InvocationTargetException e) {
                    QueryException qe = new QueryException(DatawaveErrorCode.LARGE_FIELDED_LIST_ERROR, e);
                    throw new DatawaveFatalQueryException(qe);
                }

                // add in any unused eq nodes
                if (eqNodes != null) {
                    copyChildren(eqNodes, children, data);
                }

                // add in any unused range nodes
                copyChildren(rangeNodes, children, data);

                children.addAll(markers);
            }
            // else simply add the subset back into the children list
            else {
                // recurse on the eq children in this subset
                copyChildren(eqNodes, children, data);
                track(data, field, eqNodes.size() - 1);

                // recurse on the range children in this subset
                copyChildren(rangeNodes, children, data);

                int numBatches = getBatchCount(rangeNodes.size());
                track(data, field, rangeNodes.size() - numBatches);
            }
        }

        return children.size() == 1 ? Iterables.getOnlyElement(children) : setChildren(newNode, children.toArray(new JexlNode[0]));
    }

    /**
     * Given a number of ranges to combine into ivarators return the number of batches that should be used.
     *
     * @param ranges
     *            the number of ranges to combine into ivarators
     * @return one or more batches the ranges should be split into for ivarating
     */
    private int getBatchCount(int ranges) {
        int numBatches = (int) Math.ceil(ranges / (double) Math.max(1, config.getMaxRangesPerRangeIvarator()));
        return Math.min(Math.max(1, config.getMaxOrRangeIvarators()), numBatches);
    }

    /**
     * If data is set update it for the given field to include the new possible reduction, otherwise do nothing
     *
     * @param data
     *            the data object to store the reduction per field in, may be null
     * @param field
     *            the field that can be reduced
     * @param reduction
     *            the reduction to the query term count
     */
    private void track(Object data, String field, int reduction) {
        if (data instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String,Integer> trackingMap = (Map<String,Integer>) data;
            Integer count = 0;
            if (trackingMap.get(field) != null) {
                count = trackingMap.get(field);
            }

            count += reduction;
            trackingMap.put(field, count);
        }
    }

    private List<List<Map.Entry<Range,JexlNode>>> batchRanges(TreeMap<Range,JexlNode> ranges, int numBatches) {
        List<List<Map.Entry<Range,JexlNode>>> batchedRanges = new ArrayList<>();
        double rangesPerBatch = ((double) ranges.size()) / ((double) numBatches);
        double total = rangesPerBatch;
        List<Map.Entry<Range,JexlNode>> rangeList = new ArrayList<>();
        int rangeIdx = 0;
        for (Map.Entry<Range,JexlNode> range : ranges.entrySet()) {
            if (rangeIdx++ >= total) {
                total += rangesPerBatch;
                batchedRanges.add(rangeList);
                rangeList = new ArrayList<>();
            }
            rangeList.add(range);
        }

        if (!rangeList.isEmpty())
            batchedRanges.add(rangeList);

        return batchedRanges;
    }

    private void copyChildren(Collection<JexlNode> children, Collection<JexlNode> copiedChildren, Object data) {
        for (JexlNode child : children) {
            JexlNode copiedChild = (JexlNode) child.jjtAccept(this, data);
            if (copiedChild != null) {
                copiedChildren.add(copiedChild);
            }
        }
    }

    protected Range rangeNodeToRange(JexlNode node) {
        LiteralRange range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            return new Range(new Key(String.valueOf(range.getLower())), range.isLowerInclusive(), new Key(String.valueOf(range.getUpper())),
                            range.isUpperInclusive());
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
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(subNode);
        if (subNode instanceof ASTEQNode) {
            String identifier = JexlASTHelper.getIdentifier(subNode, false);
            if (identifier != null) {
                eqNodes.put(JexlASTHelper.getIdentifier(subNode, false), origNode);
            } else {
                otherNodes.add(origNode);
            }
        } else if (instance.isType(EXCEEDED_VALUE)) {
            assignNodeByField(origNode, instance.getSource(), eqNodes, rangeNodes, otherNodes);
        } else if (instance.isType(BOUNDED_RANGE)) {
            LiteralRange range = JexlASTHelper.findRange().getRange(subNode);
            rangeNodes.put(JexlASTHelper.rebuildIdentifier(range.getFieldName()), origNode);
        } else if ((subNode.jjtGetNumChildren() == 1)
                        && (subNode instanceof ASTReferenceExpression || subNode instanceof ASTReference || subNode instanceof ASTAndNode)) {
            assignNodeByField(origNode, subNode.jjtGetChild(0), eqNodes, rangeNodes, otherNodes);
        } else {
            otherNodes.add(origNode);
        }
    }

    protected URI createFst(SortedSet<String> values) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
                    NoSuchMethodException, InvocationTargetException {
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
            codec = clazz.getDeclaredConstructor().newInstance();
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
