package datawave.query.index.lookup;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_TERM;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.INDEX_HOLE;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.base.Function;

import datawave.core.query.configuration.Result;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;

/**
 * Parses entries returned from an index lookup, see {@link RangeStream#visit(ASTEQNode, Object)}.
 *
 * An entry is defined as a Tuple of the key's column qualifier and it's {@link IndexInfo}
 *
 * A delayed predicate node is build if the IndexInfo does not have any document ids or if the column qualifier indicates a day range.
 */
public class EntryParser implements Function<Result,Tuple2<String,IndexInfo>> {
    protected ASTEQNode currNode;

    protected String fieldName;

    protected String literal;

    private boolean skipNodeDelay;

    private Set<String> indexOnlyFields = null;
    private static final Logger log = Logger.getLogger(EntryParser.class);

    public EntryParser(ASTEQNode node, String fieldName, String literal) {
        currNode = node;
        this.fieldName = fieldName;
        this.literal = literal;
        this.skipNodeDelay = false;
    }

    public EntryParser(String fieldName, String literal, boolean skipNodeDelay) {
        this((ASTEQNode) JexlNodeFactory.buildEQNode(fieldName, literal), fieldName, literal);
        this.skipNodeDelay = skipNodeDelay;
    }

    public EntryParser(ASTEQNode node, String fieldName, String literal, Set<String> indexOnlyFields) {
        this(node, fieldName, literal);
        this.indexOnlyFields = indexOnlyFields;
    }

    @Override
    public Tuple2<String,IndexInfo> apply(Result entry) {
        IndexInfo info = new IndexInfo();
        try {
            info.readFields(new DataInputStream(new ByteArrayInputStream(entry.getValue().get())));
        } catch (IOException e) {
            return null;
        }
        String date = entry.getKey().getColumnQualifier().toString();

        if (log.isTraceEnabled()) {
            log.trace("Adding " + currNode + " to " + entry.getKey() + " ");
            for (IndexMatch match : info.uids()) {
                log.trace(date + " " + match.getUid().split("\u0000")[1]);
            }
        }

        if (!skipNodeDelay && ShardEquality.isDay(date) && info.uids().isEmpty()) {

            if (isDelayedPredicate(currNode)) {
                if (log.isTraceEnabled()) {
                    log.trace("not delaying " + currNode + " because it is already delayed" + currNode.jjtGetParent() + "<- parent "
                                    + JexlStringBuildingVisitor.buildQuery(currNode) + " " + date + " " + info.uids.size());
                }
                info.applyNode(currNode);
            } else if (null != indexOnlyFields && indexOnlyFields.contains(fieldName)) {
                if (log.isTraceEnabled()) {
                    log.trace("not delaying " + currNode + " because it is index only");
                }
                info.applyNode(currNode);
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("delaying " + currNode + " because it is already delayed" + currNode.jjtGetParent() + "<- parent "
                                    + JexlStringBuildingVisitor.buildQuery(currNode) + " " + date + " " + info.uids.size());
                }
                info.applyNode(QueryPropertyMarker.create(JexlNodeFactory.buildEQNode(fieldName, literal), DELAYED));
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace(date + " Size is " + info.uids.size() + " count is " + info.count);
            }
            info.applyNode(currNode);
        }
        return Tuples.tuple(entry.getKey().getColumnQualifier().toString(), info);
    }

    protected boolean isDelayedPredicate(JexlNode currNode) {
        return QueryPropertyMarker.findInstance(currNode).isAnyTypeOf(INDEX_HOLE, DELAYED, EXCEEDED_OR, EXCEEDED_TERM, EXCEEDED_VALUE);
    }
}
