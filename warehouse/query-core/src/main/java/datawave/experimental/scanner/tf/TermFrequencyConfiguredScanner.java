package datawave.experimental.scanner.tf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.experimental.iterators.TermFrequencyScanIterator;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.data.parsers.KeyParser;
import datawave.query.data.parsers.TermFrequencyKey;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.postprocessing.tf.TermOffsetMap;
import datawave.query.postprocessing.tf.TermOffsetPopulator;

/**
 * An implementation of a {@link TermFrequencyScanner} that uses a {@link TermFrequencyScanIterator} to scan TFs.
 * <p>
 * The TermFrequencyScanIterator uses the TermFrequency fields and values from the query to filter keys on the tablet server.
 * <p>
 * The TermFrequencyScanIterator can optionally be configured to perform a rolling seek through the offsets.
 */
public class TermFrequencyConfiguredScanner extends AbstractTermFrequencyScanner {

    private static final Logger log = Logger.getLogger(TermFrequencyConfiguredScanner.class);

    private boolean seekingScan = false;

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
    public TermFrequencyConfiguredScanner(AccumuloClient client, Authorizations auths, String tableName, String scanId) {
        super(client, auths, tableName, scanId);
    }

    public Map<String,Object> fetchOffsets(ASTJexlScript script, Document d, String shard, String uid) {
        Multimap<String,String> terms = TermOffsetPopulator.getTermFrequencyFieldValues(script, Collections.emptySet(), termFrequencyFields);
        TreeMultimap<String,String> sortedTerms = TreeMultimap.create(terms);
        final TermFrequencyKey parser = new TermFrequencyKey();

        long start = System.currentTimeMillis();
        try (Scanner scanner = client.createScanner(tableName, auths)) {
            IteratorSetting setting = new IteratorSetting(100, TermFrequencyScanIterator.class);
            setting.addOption(TermFrequencyScanIterator.FIELD_VALUES, TermFrequencyScanIterator.serializeFieldValue(sortedTerms));
            if (seekingScan) {
                setting.addOption(TermFrequencyScanIterator.MODE, "seek");
            }

            scanner.addScanIterator(setting);
            scanner.setRange(createRange(shard, uid));
            scanner.fetchColumnFamily(new Text("tf"));

            Map<String,TermFrequencyList> termOffsetMap = new HashMap<>();
            TermWeightPosition.Builder position = new TermWeightPosition.Builder();

            for (Map.Entry<Key,Value> entry : scanner) {
                parser.parse(entry.getKey());

                // need to add fragment to the document for the case of a non-indexed, tokenized field
                d.put(parser.getField(), new Content(parser.getValue(), entry.getKey(), true));

                TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> offsets = parseOffsetsFromValue(entry.getKey(), entry.getValue(), parser, position);

                // First time looking up this term in a field
                TermFrequencyList tfl = termOffsetMap.get(parser.getValue());
                if (null == tfl) {
                    termOffsetMap.put(parser.getValue(), new TermFrequencyList(offsets));
                } else {
                    // Merge in the offsets for the current field+term with all previous
                    // offsets from other fields in the same term
                    tfl.addOffsets(offsets);
                }
            }

            Map<String,Object> map = new HashMap<>();
            map.put(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, new TermOffsetMap(termOffsetMap));
            log.info("time to fetch " + sortedTerms.size() + " term frequency fields for document " + uid + " was " + (System.currentTimeMillis() - start)
                            + " ms");
            return map;

        } catch (TableNotFoundException e) {
            throw new RuntimeException("Failed to fetch offsets for " + shard + " " + uid);
        }
    }

    private Range createRange(String shard, String uid) {
        Key start = new Key(shard, "tf", uid + '\0');
        Key stop = new Key(shard, "tf", uid + '\1');
        return new Range(start, true, stop, false);
    }

    private TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> parseOffsetsFromValue(Key key, Value value, KeyParser parser,
                    TermWeightPosition.Builder position) {
        TreeMultimap<TermFrequencyList.Zone,TermWeightPosition> offsets = TreeMultimap.create();
        try {
            TermWeight.Info twInfo = TermWeight.Info.parseFrom(value.get());
            TermFrequencyList.Zone twZone = new TermFrequencyList.Zone(parser.getField(), true, TermFrequencyList.getEventId(key));

            for (int i = 0; i < twInfo.getTermOffsetCount(); i++) {
                position.setTermWeightOffsetInfo(twInfo, i);
                offsets.put(twZone, position.build());
                position.reset();
            }

            return offsets;

        } catch (InvalidProtocolBufferException e) {
            log.error("Could not deserialize TermWeight protocol buffer for: " + key);
            return TreeMultimap.create();
        }
    }

    public boolean isSeekingScan() {
        return seekingScan;
    }

    public void setSeekingScan(boolean seekingScan) {
        this.seekingScan = seekingScan;
    }
}
