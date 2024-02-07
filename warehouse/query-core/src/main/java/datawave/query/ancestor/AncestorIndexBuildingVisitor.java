package datawave.query.ancestor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import datawave.query.Constants;
import datawave.query.function.Equality;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.tld.TLD;
import datawave.query.util.IteratorToSortedKeyValueIterator;

/**
 * Custom IndexBuildingVisitor that will expand (simulate) fi indexes into the entire branch of the document
 */
public class AncestorIndexBuildingVisitor extends IteratorBuildingVisitor {
    private static final Logger log = Logger.getLogger(AncestorIndexBuildingVisitor.class);

    private Map<String,List<String>> familyTreeMap = new HashMap<>();
    private Map<String,Long> timestampMap = new HashMap<>();
    private Equality equality;

    public AncestorIndexBuildingVisitor setEquality(Equality equality) {
        this.equality = equality;
        return this;
    }

    @Override
    protected SortedKeyValueIterator<Key,Value> getSourceIterator(final ASTEQNode node, boolean negation) {

        SortedKeyValueIterator<Key,Value> kvIter = null;
        try {
            if (limitLookup && !negation) {
                final String identifier = JexlASTHelper.getIdentifier(node);
                if (!disableFiEval && fieldsToAggregate.contains(identifier)) {
                    final SortedKeyValueIterator<Key,Value> baseIterator = source.deepCopy(env);
                    kvIter = new AncestorChildExpansionIterator(baseIterator, getMembers(), equality);
                    seekIndexOnlyDocument(kvIter, node);
                } else {
                    kvIter = new IteratorToSortedKeyValueIterator(getNodeEntry(node).iterator());
                }
            } else {
                kvIter = source.deepCopy(env);
                seekIndexOnlyDocument(kvIter, node);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return kvIter;
    }

    @Override
    protected void seekIndexOnlyDocument(SortedKeyValueIterator<Key,Value> kvIter, ASTEQNode node) throws IOException {
        if (null != rangeLimiter && limitLookup) {
            Key startKey = getKey(node);
            Key endKey = getEndKey(node);

            kvIter.seek(new Range(startKey, true, endKey, true), Collections.emptyList(), false);
        }
    }

    protected Key getEndKey(JexlNode node) {
        Key endKey = rangeLimiter.getEndKey();
        String identifier = JexlASTHelper.getIdentifier(node);
        Object objValue = JexlASTHelper.getLiteralValue(node);
        String value = null == objValue ? "null" : objValue.toString();

        StringBuilder builder = new StringBuilder("fi");
        builder.append(NULL_DELIMETER).append(identifier);

        Text cf = new Text(builder.toString());

        builder = new StringBuilder(value);

        builder.append(NULL_DELIMETER).append(endKey.getColumnFamily());
        Text cq = new Text(builder.toString());

        return new Key(endKey.getRow(), cf, cq, endKey.getTimestamp());
    }

    private List<String> getMembers() {
        Range wholeDocRange = getWholeDocRange(rangeLimiter);
        final String tld = getTLDId(wholeDocRange.getStartKey());
        final String dataType = getDataType(wholeDocRange.getStartKey());
        List<String> members = familyTreeMap.get(tld);

        // use the cached tree if available
        if (members == null) {
            SortedKeyValueIterator<Key,Value> kvIter = source.deepCopy(env);
            members = getMembers(wholeDocRange.getStartKey().getRow().toString(), tld, dataType, kvIter);

            // set the members for later use
            familyTreeMap.put(tld, members);
        }

        return members;
    }

    /**
     * Expand node entry from the single fi that is generated by this node, and instead generate keys for the entire document branch
     *
     * @param node
     *            the equal node
     * @return a set of keys for the document
     */
    @Override
    protected Collection<Map.Entry<Key,Value>> getNodeEntry(ASTEQNode node) {
        final List<Map.Entry<Key,Value>> keys = new ArrayList<>();
        Range wholeDocRange = getWholeDocRange(rangeLimiter);
        final String tld = getTLDId(wholeDocRange.getStartKey());
        final String dataType = getDataType(wholeDocRange.getStartKey());
        List<String> members = familyTreeMap.get(tld);

        // use the cached tree if available
        if (members == null) {
            SortedKeyValueIterator<Key,Value> kvIter = source.deepCopy(env);
            members = getMembers(wholeDocRange.getStartKey().getRow().toString(), tld, dataType, kvIter);

            // set the members for later use
            familyTreeMap.put(tld, members);
        }

        for (String uid : members) {
            // only generate index keys beyond the current uid in the tree
            Key rangeCheckKey = new Key(rangeLimiter.getStartKey().getRow().toString(), dataType + Constants.NULL_BYTE_STRING + uid);
            if (!rangeLimiter.beforeStartKey(rangeCheckKey) && !rangeLimiter.afterEndKey(rangeCheckKey)) {
                Long timestamp = timestampMap.get(uid);
                if (timestamp == null) {
                    timestamp = rangeLimiter.getStartKey().getTimestamp();
                }
                keys.add(Maps.immutableEntry(getKey(node, rangeLimiter.getStartKey().getRow(), dataType, uid, timestamp), Constants.NULL_VALUE));
            }
        }

        return keys;
    }

    /**
     * Get all uids for a given tldUid and dataType and row from the iterator, seeking between keys
     *
     * @param row
     *            a row
     * @param tldUid
     *            the TLD uid
     * @param dataType
     *            the data type
     * @param iterator
     *            an iterator
     * @return a list of uids
     */
    private List<String> getMembers(String row, String tldUid, String dataType, SortedKeyValueIterator<Key,Value> iterator) {
        final List<String> members = new ArrayList<>();
        Key startKey = new Key(row, dataType + Constants.NULL_BYTE_STRING + tldUid);
        Key endKey = new Key(row, dataType + Constants.NULL_BYTE_STRING + tldUid + Constants.MAX_UNICODE_STRING);

        // inclusive to catch the first uid
        Range range = new Range(startKey, true, endKey, false);
        try {
            iterator.seek(range, Collections.emptyList(), false);

            while (iterator.hasTop()) {
                Key nextKey = iterator.getTopKey();
                String keyTld = getTLDId(nextKey);
                if (keyTld.equals(tldUid)) {
                    String uid = getUid(nextKey);
                    members.add(uid);
                    timestampMap.put(uid, nextKey.getTimestamp());
                } else {
                    break;
                }

                // seek to the next child by shifting the startKey
                startKey = new Key(row, nextKey.getColumnFamily() + Constants.NULL_BYTE_STRING);
                iterator.seek(new Range(startKey, true, endKey, true), Collections.emptyList(), false);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return members;
    }

    /**
     * Extract the data type from an fi key
     *
     * @param key
     *            the key
     * @return a data type string
     */
    private String getDataType(Key key) {
        String cf = key.getColumnFamily().toString();
        final int splitIndex = cf.indexOf('\0');

        if (splitIndex > -1) {
            return cf.substring(0, splitIndex);
        }

        return null;
    }

    /**
     * Extract the uid from an event key, format shardId dataType\0UID FieldName\0FieldValue NULL
     *
     * @param key
     *            a key
     * @return the uid
     */
    private String getUid(Key key) {
        Text startColfam = key.getColumnFamily();
        if (startColfam.find(Constants.NULL) != -1) {
            // have a start key with a document uid, add to the end of the cf to ensure we go to the next doc
            // parse out the uid
            String cf = startColfam.toString();
            int index = cf.indexOf('\0');
            if (index >= 0) {

                return cf.substring(index + 1);
            }
        }

        return null;
    }

    /**
     * Extract the TLD uid from an event key
     *
     * @param key
     *            a key
     * @return the TLD uid
     */
    private String getTLDId(Key key) {
        String uid = getUid(key);

        // if the uid is not empty
        if (!uid.isEmpty()) {
            uid = TLD.parseRootPointerFromId(uid);
        }

        return uid;
    }

    /**
     * Expand a range to include an entire document if it includes a specific event
     *
     * @param r
     *            the range
     * @return the expanded range
     */
    protected Range getWholeDocRange(final Range r) {
        Range result = r;

        Key start = r.getStartKey();
        Key end = r.getEndKey();
        String endCf = (end == null || end.getColumnFamily() == null ? "" : end.getColumnFamily().toString());
        String startCf = (start == null || start.getColumnFamily() == null ? "" : start.getColumnFamily().toString());

        // if the end key inclusively includes a datatype\0UID or has datatype\0UID\0, then move the key past the children
        if (!endCf.isEmpty() && (r.isEndKeyInclusive() || endCf.charAt(endCf.length() - 1) == '\0')) {
            String row = end.getRow().toString().intern();
            if (endCf.charAt(endCf.length() - 1) == '\0') {
                endCf = endCf.substring(0, endCf.length() - 1);
            }
            Key postDoc = new Key(row, endCf + Character.MAX_CODE_POINT);
            result = new Range(r.getStartKey(), r.isStartKeyInclusive(), postDoc, false);
        }

        // if the start key is not inclusive, and we have a datatype\0UID, then move the start past the children thereof
        if (!r.isStartKeyInclusive() && !startCf.isEmpty()) {
            // we need to bump append 0xff to that byte array because we want to skip the children
            String row = start.getRow().toString().intern();

            Key postDoc = new Key(row, startCf + Character.MAX_CODE_POINT);
            // if this puts us past the end of teh range, then adjust appropriately
            if (result.contains(postDoc)) {
                result = new Range(postDoc, false, result.getEndKey(), result.isEndKeyInclusive());
            } else {
                result = new Range(result.getEndKey(), false, result.getEndKey().followingKey(PartialKey.ROW_COLFAM), false);
            }
        }

        return result;
    }

    /**
     * Generate a new fi key from the current node with the specific row/dataType/uid/timestamp
     *
     * @param node
     *            the jexl node
     * @param row
     *            the row
     * @param dataType
     *            the data type
     * @param uid
     *            the uid
     * @param timestamp
     *            a timestamp
     * @return a fi key
     */
    private Key getKey(JexlNode node, Text row, String dataType, String uid, long timestamp) {
        String fieldName = JexlASTHelper.getIdentifier(node);
        Object objValue = JexlASTHelper.getLiteralValue(node);
        String fieldValuie = null == objValue ? "null" : objValue.toString();

        StringBuilder builder = new StringBuilder("fi");
        builder.append(NULL_DELIMETER).append(fieldName);
        Text cf = new Text(builder.toString());

        builder = new StringBuilder(fieldValuie);
        builder.append(NULL_DELIMETER).append(dataType).append(NULL_DELIMETER).append(uid);
        Text cq = new Text(builder.toString());

        return new Key(row, cf, cq, timestamp);
    }
}
