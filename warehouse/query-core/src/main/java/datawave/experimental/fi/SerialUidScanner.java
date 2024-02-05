package datawave.experimental.fi;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAnnotatedStatement;
import org.apache.commons.jexl3.parser.ASTAnnotation;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTBreak;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTContinue;
import org.apache.commons.jexl3.parser.ASTDecrementGetNode;
import org.apache.commons.jexl3.parser.ASTDefineVars;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTDoWhileStatement;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEWNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTExtendedLiteral;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTGetDecrementNode;
import org.apache.commons.jexl3.parser.ASTGetIncrementNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTIncrementGetNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTJxltLiteral;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNEWNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNSWNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNullpNode;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTQualifiedIdentifier;
import org.apache.commons.jexl3.parser.ASTRangeNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTRegexLiteral;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSWNode;
import org.apache.commons.jexl3.parser.ASTSetAddNode;
import org.apache.commons.jexl3.parser.ASTSetAndNode;
import org.apache.commons.jexl3.parser.ASTSetDivNode;
import org.apache.commons.jexl3.parser.ASTSetLiteral;
import org.apache.commons.jexl3.parser.ASTSetModNode;
import org.apache.commons.jexl3.parser.ASTSetMultNode;
import org.apache.commons.jexl3.parser.ASTSetOrNode;
import org.apache.commons.jexl3.parser.ASTSetShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSetSubNode;
import org.apache.commons.jexl3.parser.ASTSetXorNode;
import org.apache.commons.jexl3.parser.ASTShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTShiftRightNode;
import org.apache.commons.jexl3.parser.ASTShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTUnaryPlusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.experimental.intersect.UidIntersection;
import datawave.experimental.intersect.UidIntersectionStrategy;
import datawave.experimental.visitor.QueryTermVisitor;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Serially scans the field index for a set of query terms.
 */
public class SerialUidScanner extends AbstractUidScanner {
    private static final Logger log = Logger.getLogger(SerialUidScanner.class);

    public SerialUidScanner(AccumuloClient client, Authorizations auth, String tableName, String scanId) {
        super(client, auth, tableName, scanId);
    }

    /**
     * Scans the field index serially for the set of query terms
     */
    @Override
    public SortedSet<String> scan(ASTJexlScript script, String row, Set<String> indexedFields) {
        Set<JexlNode> terms = QueryTermVisitor.parse(script);
        long elapsed = System.currentTimeMillis();
        int count = 0;
        Map<String,Set<String>> nodesToUids = new HashMap<>();
        for (JexlNode term : terms) {
            String field;
            if (term instanceof ASTAndNode) {
                // handle the bounded range case
                field = JexlASTHelper.getIdentifiers(term).get(0).getName();
            } else {
                field = JexlASTHelper.getIdentifier(term);

                // FIELD == null is handled at evaluation time, no field index work required
                try {
                    String value = (String) JexlASTHelper.getLiteralValue(term);
                    if (value == null) {
                        continue;
                    }
                } catch (NoSuchElementException e) {
                    continue;
                }
            }

            if (indexedFields.contains(field)) {
                count++;
                Set<String> uids = scanFieldIndexForTerm(row, field, term);
                String key = JexlStringBuildingVisitor.buildQueryWithoutParse(term);
                nodesToUids.put(key, uids);
            } else {
                log.info("Field " + field + " is not indexed, do not look for it in the field index");
            }
        }

        if (logStats) {
            elapsed = System.currentTimeMillis() - elapsed;
            log.info("scanned field index for " + count + "/" + terms.size() + " indexed terms in " + elapsed + " ms");
        }

        UidIntersectionStrategy intersector = new UidIntersection();
        return intersector.intersect(script, nodesToUids);
    }

    @Override
    public void setLogStats(boolean logStats) {
        this.logStats = logStats;
    }

    /**
     * Scan the field index for the provided term
     *
     * @param shard
     *            the shard
     * @param field
     *            the field
     * @param term
     *            a JexlNode representing the field and value
     * @return a set of uids
     */
    protected Set<String> scanFieldIndexForTerm(String shard, String field, JexlNode term) {
        Set<String> uids = new HashSet<>();
        Range range = rangeBuilder.rangeFromTerm(shard, term);

        try (Scanner scanner = client.createScanner(tableName, auths)) {
            scanner.setRange(range);
            if (!field.equals(Constants.ANY_FIELD)) {
                scanner.fetchColumnFamily(new Text("fi\0" + field));
            } else {
                throw new IllegalStateException("Should never have _ANYFIELD_ in this stage.");
            }

            for (Map.Entry<Key,Value> entry : scanner) {
                if (timeFilter.apply(entry.getKey())) {
                    uids.add(dtUidFromKey(entry.getKey()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
        }
        return uids;
    }

    /**
     * Extract the datatype and uid from the field index key
     *
     * @param key
     *            a field index key
     * @return the datatype and uid
     */
    private String dtUidFromKey(Key key) {
        String cq = key.getColumnQualifier().toString();
        int index = cq.indexOf('\u0000');
        return cq.substring(index + 1);
    }

    @Override
    protected Object visit(ASTJexlScript node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTBlock node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTIfStatement node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTWhileStatement node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTDoWhileStatement node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTContinue node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTBreak node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTForeachStatement node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTReturnStatement node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTAssignment node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTVar node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTDefineVars node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTReference node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTTernaryNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTNullpNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTOrNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTAndNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTBitwiseOrNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTBitwiseXorNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTBitwiseAndNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTShiftLeftNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTShiftRightNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTShiftRightUnsignedNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTEQNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTNENode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTLTNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTGTNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTLENode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTGENode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTERNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTNRNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSWNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTNSWNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTEWNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTNEWNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTAddNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSubNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTMulNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTDivNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTModNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTUnaryMinusNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTUnaryPlusNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTBitwiseComplNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTNotNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTIdentifier node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTNullLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTTrueNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTFalseNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTNumberLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTStringLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTRegexLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTExtendedLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTArrayLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTRangeNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTMapLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTMapEntry node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTEmptyFunction node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSizeFunction node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTFunctionNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTMethodNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTConstructorNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTArrayAccess node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTIdentifierAccess node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTArguments node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTReferenceExpression node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetAddNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetSubNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetMultNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetDivNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetModNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetAndNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetOrNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetXorNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetShiftLeftNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetShiftRightNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTSetShiftRightUnsignedNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTGetDecrementNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTGetIncrementNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTDecrementGetNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTIncrementGetNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTJxltLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTAnnotation node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTAnnotatedStatement node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTQualifiedIdentifier node, Object data) {
        return null;
    }
}
