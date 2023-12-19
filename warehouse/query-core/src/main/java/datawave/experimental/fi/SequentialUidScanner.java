package datawave.experimental.fi;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
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

public class SequentialUidScanner extends AbstractUidScanner {

    private static final Logger log = Logger.getLogger(SequentialUidScanner.class);

    public SequentialUidScanner(AccumuloClient client, Authorizations auths, String tableName, String scanId) {
        super(client, auths, tableName, scanId);
    }

    @Override
    public Set<String> scan(ASTJexlScript script, String row, Set<String> indexedFields) {
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
}
