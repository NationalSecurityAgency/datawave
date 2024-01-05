package datawave.experimental.scanner.tf;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.experimental.util.ScanStats;
import datawave.experimental.visitor.QueryTermVisitor;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.postprocessing.tf.Function;
import datawave.query.postprocessing.tf.TermOffsetMap;
import datawave.query.postprocessing.tf.TermOffsetPopulator;

/**
 * An implementation of a {@link TermFrequencyScanner} that executes a series of individual scans in sequence, one after another.
 */
public class TermFrequencySequentialScanner extends AbstractTermFrequencyScanner {

    private static final Logger log = Logger.getLogger(TermFrequencySequentialScanner.class);

    private static final String TF_STRING = "tf";
    private static final Text TF_CF = new Text(TF_STRING);
    private static final String NULL_BYTE = "\0";

    private final ScanStats scanStats = new ScanStats();

    /**
     * Delegates to default abstract constructor
     *
     * @param client
     *            an accumulo client
     * @param auths
     *            authorizations
     * @param tableName
     *            the table name
     * @param scanId
     *            an id associated with the scan
     */
    public TermFrequencySequentialScanner(AccumuloClient client, Authorizations auths, String tableName, String scanId) {
        super(client, auths, tableName, scanId);
    }

    @Override
    public Map<String,Object> fetchOffsets(ASTJexlScript script, Document d, String shard, String uid) {
        long start = System.currentTimeMillis();
        // first check for a content function, i.e. that the query actually requires offsets
        Multimap<String,Function> functionMap = TermOffsetPopulator.getContentFunctions(script);

        // by default just fetch all
        Set<String> args = new HashSet<>();
        for (String key : functionMap.keySet()) {
            Collection<Function> functions = functionMap.get(key);
            for (Function function : functions) {
                for (JexlNode arg : function.args()) {
                    args.add(JexlStringBuildingVisitor.buildQueryWithoutParse(arg));
                }
            }
        }

        Set<JexlNode> queryTerms = QueryTermVisitor.parse(script);
        Set<JexlNode> tfTerms = new HashSet<>();

        for (JexlNode term : queryTerms) {
            String built = JexlStringBuildingVisitor.buildQueryWithoutParse(term);
            boolean containsTokenizedField = false;
            for (String tfField : termFrequencyFields) {
                if (built.contains(tfField)) {
                    containsTokenizedField = true;
                    break;
                }
            }

            boolean containsContentFunctionArg = false;
            for (String arg : args) {
                if (built.contains(arg)) {
                    containsContentFunctionArg = true;
                    break;
                }
            }

            if (containsTokenizedField || containsContentFunctionArg) {
                tfTerms.add(term);
            }
        }

        // fetch all tf args
        Map<String,TermFrequencyList> termOffsetMap = new HashMap<>();
        for (JexlNode term : tfTerms) {
            String field = JexlASTHelper.getIdentifier(term);
            String value = (String) JexlASTHelper.getLiteralValue(term);

            TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> offsets = fetchOffset(shard, field, value, uid, d);
            if (!offsets.isEmpty()) {
                TermFrequencyList tfl = termOffsetMap.get(value);
                if (null == tfl) {
                    termOffsetMap.put(value, new TermFrequencyList(offsets));
                } else {
                    // Merge in the offsets for the current field+term with all previous
                    // offsets from other fields in the same term
                    tfl.addOffsets(offsets);
                }
            }
        }

        // put term offset map
        Map<String,Object> map = new HashMap<>();
        map.put(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, new TermOffsetMap(termOffsetMap));

        if (logStats) {
            log.info("time to fetch " + tfTerms.size() + " term frequency fields for document " + uid + " was " + (System.currentTimeMillis() - start) + " ms");
        }

        return map;
    }

    // TODO -- make this a batch scanner instead and set all ranges up front
    private TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> fetchOffset(String shard, String field, String value, String uid, Document d) {
        TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> offsets = TreeMultimap.create();
        Range range = buildTermFrequencyRange(shard, field, value, uid);
        try (Scanner scanner = client.createScanner(tableName, auths)) {
            scanner.setRange(range);
            scanner.fetchColumnFamily(TF_CF);
            for (Map.Entry<Key,Value> entry : scanner) {
                TermWeight.Info twInfo = TermWeight.Info.parseFrom(entry.getValue().get());
                TermFrequencyList.Zone twZone = new TermFrequencyList.Zone(field, true, uid);
                TermWeightPosition.Builder position = new TermWeightPosition.Builder();
                for (int i = 0; i < twInfo.getTermOffsetCount(); i++) {
                    position.setTermWeightOffsetInfo(twInfo, i);
                    offsets.put(twZone, position.build());
                    position.reset();
                }

                // need to add fragment to the document for the case of a non-indexed, tokenized field
                d.put(field, new Content(value, entry.getKey(), true));
            }
        } catch (TableNotFoundException | InvalidProtocolBufferException e) {
            e.printStackTrace();
            log.error("exception while fetching tf offsets: " + e.getMessage());
        }
        return offsets;
    }

    private Range buildTermFrequencyRange(String shard, String field, String value, String uid) {
        boolean isTld = false;
        Key startKey = new Key(shard, TF_STRING, uid + NULL_BYTE + value + NULL_BYTE + field);
        Key endKey;
        if (isTld) {
            endKey = null;
        } else {
            endKey = startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        }
        return new Range(startKey, true, endKey, false);
    }
}
