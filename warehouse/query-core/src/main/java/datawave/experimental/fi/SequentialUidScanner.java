package datawave.experimental.fi;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.experimental.intersect.UidIntersection;
import datawave.experimental.intersect.UidIntersectionStrategy;
import datawave.experimental.iterators.FieldIndexScanIterator;
import datawave.experimental.visitor.QueryTermVisitor;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Remote scan
 */
public class SequentialUidScanner extends AbstractUidScanner {

    private static final Logger log = Logger.getLogger(SequentialUidScanner.class);

    public SequentialUidScanner(AccumuloClient client, Authorizations auths, String tableName, String scanId) {
        super(client, auths, tableName, scanId);
    }

    @Override
    public SortedSet<String> scan(ASTJexlScript script, String row, Set<String> indexedFields) {
        long start = System.currentTimeMillis();
        Set<JexlNode> terms = QueryTermVisitor.parse(script);
        TreeMultimap<String,String> map = termsToMap(terms, indexedFields);
        Map<String,JexlNode> keyToNodeMap = new HashMap<>();
        for (JexlNode term : terms) {
            keyToNodeMap.put(JexlStringBuildingVisitor.buildQueryWithoutParse(term), term);
        }

        try (Scanner scanner = client.createScanner(tableName, auths)) {
            IteratorSetting setting = new IteratorSetting(100, FieldIndexScanIterator.class);
            setting.addOption(FieldIndexScanIterator.FIELD_VALUES, FieldIndexScanIterator.serializeFieldValue(map));

            scanner.addScanIterator(setting);
            scanner.setRange(createRange(row));
            for (String field : map.keySet()) {
                scanner.fetchColumnFamily(new Text("fi\0" + field));
            }

            Multimap<String,String> uidMap = HashMultimap.create();
            FieldIndexKey parser = new FieldIndexKey();

            for (Map.Entry<Key,Value> entry : scanner) {
                parser.parse(entry.getKey());
                String key = parser.getField() + " == '" + parser.getValue() + "'";
                uidMap.put(key, parser.getDatatype() + '\u0000' + parser.getUid());
            }

            Map<String,Set<String>> value = new HashMap<>();
            for (String key : uidMap.keySet()) {
                value.put(key, new HashSet<>(uidMap.get(key)));
            }

            long elapsed = System.currentTimeMillis() - start;
            log.info("scanned field index for " + map.keySet().size() + "/" + terms.size() + " indexed terms in " + elapsed + " ms");
            UidIntersectionStrategy intersector = new UidIntersection();
            return intersector.intersect(script, value);

        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setLogStats(boolean logStats) {
        // no op
    }

    private TreeMultimap<String,String> termsToMap(Set<JexlNode> terms, Set<String> indexedFields) {
        TreeMultimap<String,String> map = TreeMultimap.create();
        for (JexlNode term : terms) {
            if (!(term instanceof ASTAndNode)) {
                String field = JexlASTHelper.getIdentifier(term);
                if (indexedFields.contains(field)) {
                    String value = (String) JexlASTHelper.getLiteralValue(term);
                    map.put(field, value);
                }
            }
        }
        return map;
    }

    private Range createRange(String shard) {
        Key start = new Key(shard, "fi\0");
        Key end = new Key(shard, "fi\u0000\uffff");
        return new Range(start, true, end, false);
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
