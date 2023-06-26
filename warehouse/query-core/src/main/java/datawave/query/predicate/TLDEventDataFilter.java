package datawave.query.predicate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import datawave.query.tld.TLD;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query except for the root document in which case all fields
 * are returned.
 */
public class TLDEventDataFilter extends EventDataQueryExpressionFilter {

    public static final byte[] FI_CF = new Text("fi").getBytes();
    public static final byte[] TF_CF = Constants.TERM_FREQUENCY_COLUMN_FAMILY.getBytes();

    private final long maxFieldsBeforeSeek;
    private final long maxKeysBeforeSeek;

    // track whitelist/blacklist if specified
    private List<String> sortedWhitelist = null;
    private List<String> sortedBlacklist = null;

    // track query fields (must be sorted)
    protected List<String> queryFields;

    // track recently seen key fields
    private String lastField;
    private long fieldCount = 0;

    // track last list index
    private int lastListSeekIndex = -1;
    private long keyMissCount;

    // track recently parsed key for fast(er) evaluation
    private ParseInfo lastParseInfo;

    /**
     * track count limits per field, _ANYFIELD_ implies a constraint on all fields
     */
    private Map<String,Integer> limitFieldsMap;

    /**
     * if _ANYFIELD_ appears in the limitFieldsMap this will be set to that value or -1 if not configured
     */
    private int anyFieldLimit;

    private Set<String> nonEventFields;

    public TLDEventDataFilter(ASTJexlScript script, Set<String> queryFields, TypeMetadata attributeFactory, Set<String> whitelist, Set<String> blacklist,
                    long maxFieldsBeforeSeek, long maxKeysBeforeSeek) {
        this(script, queryFields, attributeFactory, whitelist, blacklist, maxFieldsBeforeSeek, maxKeysBeforeSeek, Collections.EMPTY_MAP, null,
                        Collections.EMPTY_SET);
    }

    /**
     * Field which should be used when transform() is called on a rejected Key that is field limited to store the field
     */
    private String limitFieldsField = null;

    /**
     * Initialize the query field filter with all of the fields required to evaluation this query
     *
     * @param script
     *            - script
     * @param attributeFactory
     *            - attributeFactory
     * @param blacklist
     *            - blacklist
     * @param limitFieldsField
     *            - limitFieldsField
     * @param limitFieldsMap
     *            - limitFieldsMap
     * @param maxFieldsBeforeSeek
     *            - maxFieldsBeforeSeek
     * @param maxKeysBeforeSeek
     *            - maxFieldsBeforeSeek
     * @param nonEventFields
     *            - nonEventFields
     * @param queryFields
     *            - queryFields
     * @param whitelist
     *            - whitelist
     */
    public TLDEventDataFilter(ASTJexlScript script, Set<String> queryFields, TypeMetadata attributeFactory, Set<String> whitelist, Set<String> blacklist,
                    long maxFieldsBeforeSeek, long maxKeysBeforeSeek, Map<String,Integer> limitFieldsMap, String limitFieldsField, Set<String> nonEventFields) {
        super(script, attributeFactory, nonEventFields);

        this.maxFieldsBeforeSeek = maxFieldsBeforeSeek;
        this.maxKeysBeforeSeek = maxKeysBeforeSeek;
        this.limitFieldsMap = Collections.unmodifiableMap(limitFieldsMap);
        this.limitFieldsField = limitFieldsField;
        this.nonEventFields = nonEventFields;

        // set the anyFieldLimit once if specified otherwise set to -1
        anyFieldLimit = limitFieldsMap.get(Constants.ANY_FIELD) != null ? limitFieldsMap.get(Constants.ANY_FIELD) : -1;

        setQueryFields(queryFields, script);
        updateLists(whitelist, blacklist);
        setSortedLists(whitelist, blacklist);
    }

    public TLDEventDataFilter(TLDEventDataFilter other) {
        super(other);
        maxFieldsBeforeSeek = other.maxFieldsBeforeSeek;
        maxKeysBeforeSeek = other.maxKeysBeforeSeek;
        sortedWhitelist = other.sortedWhitelist;
        sortedBlacklist = other.sortedBlacklist;
        queryFields = other.queryFields;
        lastField = other.lastField;
        fieldCount = other.fieldCount;
        lastListSeekIndex = other.lastListSeekIndex;
        keyMissCount = other.keyMissCount;
        if (other.lastParseInfo != null) {
            lastParseInfo = new ParseInfo(other.lastParseInfo);
        }
        limitFieldsField = other.limitFieldsField;
        limitFieldsMap = other.limitFieldsMap;
        anyFieldLimit = other.anyFieldLimit;
        nonEventFields = other.nonEventFields;
    }

    @Override
    public void startNewDocument(Key document) {
        super.startNewDocument(document);
        // clear the parse info so a length comparison can't be made against a new document
        lastParseInfo = null;
        // reset the seek index so the first call to getListSeek() will return the first field
        lastListSeekIndex = -1;
    }

    /**
     * Keep for context evaluation and potential return to the client. If a Key returns false the Key will not be used for context evaluation or returned to the
     * client. If a Key returns true but keep() returns false the document will be used for context evaluation, but will not be returned to the client. If a Key
     * returns true and keep() returns true the key will be used for context evaluation and returned to the client.
     *
     * @param input
     *            an input
     * @return true if Key should be added to context, false otherwise
     */
    @Override
    public boolean apply(Entry<Key,String> input) {
        return apply(input, true);
    }

    @Override
    public boolean peek(Entry<Key,String> input) {
        return apply(input, false);
    }

    private boolean apply(Entry<Key,String> input, boolean update) {
        // if a TLD, then accept em all, other wise defer to the query field
        // filter
        Key current = input.getKey();
        lastParseInfo = getParseInfo(current);
        boolean root = lastParseInfo.isRoot();
        boolean keep = keepField(current, update, root);
        if (keep) {
            if (root) {
                // must return true on the root or the field cannot be returned
                return true;
            } else {
                // delegate to the super
                if (update) {
                    return super.apply(input);
                } else {
                    return super.peek(input);
                }
            }
        }

        return false;
    }

    /**
     * Determine if a Key should be kept. If a Key is a part of the TLD it will always be kept as long as we have not exceeded the key count limit for that
     * field if limits are enabled. Otherwise all TLD Key's will be kept. For a non-TLD the Key will only be kept if it is a nonEvent field which will be used
     * for query evaluation (apply()==true)
     *
     * @see datawave.query.predicate.Filter#keep(Key)
     *
     * @param k
     *            a key
     * @return true to keep, false otherwise
     */
    @Override
    public boolean keep(Key k) {
        // only keep the data from the top level document with fields that matter
        lastParseInfo = getParseInfo(k);
        boolean root = lastParseInfo.isRoot();

        if (root) {
            return k.getColumnQualifier().getLength() == 0 || keepField(k, false, true);
        } else {
            return nonEventFields.contains(lastParseInfo.getField()) && keepField(k, false, false) && apply(k, false);
        }
    }

    /**
     * Test a key against the last parsed key. Return a new parsedInfo if the cached version is not the same, otherwise reuse the existing ParseInfo
     *
     * @param current
     *            the key to get ParseInfo for
     * @return the non-null ParseInfo for the Key
     */
    protected ParseInfo getParseInfo(Key current) {
        if (lastParseInfo == null || !lastParseInfo.isSame(current)) {
            // initialize the new parseInfo
            ParseInfo parseInfo = new ParseInfo(current);
            boolean root;
            // can only short-cut on CF length when dealing with an event key
            if (lastParseInfo != null && isEventKey(current)) {
                int lastLength = lastParseInfo.key.getColumnFamilyData().length();
                int currentLength = current.getColumnFamilyData().length();
                if (lastLength == currentLength) {
                    root = lastParseInfo.isRoot();
                } else if (lastLength < currentLength) {
                    // next key must be longer or it would have been sorted first within the same document
                    root = false;
                } else {
                    // the filter is being used again at the beginning of the document and state needs to be reset
                    root = isRootPointer(current);
                }
            } else {
                root = isRootPointer(current);
            }
            parseInfo.setRoot(root);
            parseInfo.setField(getCurrentField(current));

            return parseInfo;
        }

        return lastParseInfo;
    }

    protected String getUid(Key k) {
        String uid;
        String cf = k.getColumnFamily().toString();
        if (cf.equals(Constants.TERM_FREQUENCY_COLUMN_FAMILY.toString())) {
            String cq = k.getColumnQualifier().toString();
            int start = cq.indexOf('\0') + 1;
            uid = cq.substring(start, cq.indexOf('\0', start));
        } else if (cf.startsWith("fi\0")) {
            String cq = k.getColumnQualifier().toString();
            uid = cq.substring(cq.lastIndexOf('\0') + 1);
        } else {
            uid = cf.substring(cf.lastIndexOf('\0') + 1);
        }
        return uid;
    }

    private boolean isEventKey(Key k) {
        ByteSequence cf = k.getColumnFamilyData();
        return !(WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, FI_CF, 0, 2) == 0)
                        && !(WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, TF_CF, 0, 2) == 00);
    }

    public static boolean isRootPointer(Key k) {
        ByteSequence cf = k.getColumnFamilyData();

        if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, FI_CF, 0, 2) == 0) {
            ByteSequence seq = k.getColumnQualifierData();
            int i = seq.length() - 19;
            for (; i >= 0; i--) {

                if (seq.byteAt(i) == '.') {
                    return false;
                } else if (seq.byteAt(i) == 0x00) {
                    break;
                }
            }

            for (i += 20; i < seq.length(); i++) {
                if (seq.byteAt(i) == '.') {
                    return false;
                }
            }
            return true;

        } else if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, TF_CF, 0, 2) == 0) {
            ByteSequence seq = k.getColumnQualifierData();

            // work front to back, just in case the TF value includes a null byte
            boolean foundStart = false;
            int dotCount = 0;
            for (int i = 0; i < seq.length(); i++) {
                if (!foundStart && seq.byteAt(i) == 0x00) {
                    foundStart = true;
                } else if (foundStart && seq.byteAt(i) == 0x00) {
                    // end of uid, got here, is root
                    return true;
                } else if (foundStart && seq.byteAt(i) == '.') {
                    dotCount++;
                    if (dotCount > 2) {
                        return false;
                    }
                }
            }

            // can't parse
            return false;
        } else {
            int i = 0;
            for (i = 0; i < cf.length(); i++) {

                if (cf.byteAt(i) == 0x00) {
                    break;
                }
            }

            for (i += 20; i < cf.length(); i++) {

                if (cf.byteAt(i) == '.') {
                    return false;
                } else if (cf.byteAt(i) == 0x00) {
                    return true;
                }
            }
            return true;
        }

    }

    /**
     * When dealing with a root pointer, seek through any field that should be returned in the result, when dealing with a child seek to the next query field in
     * the current child
     *
     * @param current
     *            the current key at the top of the source iterator
     * @param endKey
     *            the current range endKey
     * @param endKeyInclusive
     *            the endKeyInclusive flag from the current range
     * @return the new range or null if a seek should not be performed
     */
    @Override
    public Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive) {
        Range range;
        lastParseInfo = getParseInfo(current);
        if (lastParseInfo.isRoot()) {
            range = getListSeek(current, endKey, endKeyInclusive);
        } else {
            // only look in children for query related fields
            range = getQueryFieldRange(current, endKey, endKeyInclusive);
        }

        return range;
    }

    /**
     * Look in the query fields only, regardless of whitelist or blacklist configuration
     *
     * @param current
     *            the current key
     * @param endKey
     *            the end range key
     * @param endKeyInclusive
     *            the end inclusive flag
     * @return the new range or null if a seek should not be performed
     */
    protected Range getQueryFieldRange(Key current, Key endKey, boolean endKeyInclusive) {
        Range range = null;

        // short circuit the seek if the threshold for seeking hasn't been met or it is disabled
        if (bypassSeek()) {
            return range;
        }

        final String fieldName = lastParseInfo.getField();
        // generate a whitelist seek only on the query fields, without using any previous state
        range = getWhitelistSeek(current, fieldName, endKey, endKeyInclusive, queryFields, -1);

        return range;
    }

    /**
     * As long as a seek should not be bypassed, generate either a whitelist or blacklist range
     *
     * @param current
     *            the current key
     * @param endKey
     *            the end key
     * @param endKeyInclusive
     *            the end key inclusive flag
     * @return if a seek should be performed return a non-null range, otherwise return null
     */
    protected Range getListSeek(Key current, Key endKey, boolean endKeyInclusive) {
        Range range = null;

        // short circuit the seek if the threshold for seeking hasn't been met or it is disabled
        if (bypassSeek()) {
            return range;
        }

        final String fieldName = lastParseInfo.getField();
        // first handle seek due to a field limit, then use the white/block lists if necessary
        if (isFieldLimit(fieldName)) {
            range = getFieldSeek(current, fieldName, endKey, endKeyInclusive);
        }

        // if it wasn't a field limit seek then do a normal seek
        if (range == null) {
            if (sortedWhitelist != null) {
                range = getWhitelistSeek(current, fieldName, endKey, endKeyInclusive);
            } else if (sortedBlacklist != null) {
                range = getBlacklistSeek(current, fieldName, endKey, endKeyInclusive);
            }
        }

        return range;
    }

    /**
     * Seek starting from the end of the current field
     *
     * @param current
     *            the current key
     * @param fieldName
     *            the field name to be seeked
     * @param endKey
     *            the current seek end key
     * @param endKeyInclusive
     *            the current seek end key inclusive flag
     * @return a new range that begins at the end of the current field
     */
    private Range getFieldSeek(Key current, String fieldName, Key endKey, boolean endKeyInclusive) {
        Key startKey = new Key(current.getRow(), current.getColumnFamily(), new Text(fieldName + "\u0001"));
        return new Range(startKey, true, endKey, endKeyInclusive);
    }

    /**
     * Seek using the main sorted whitelist and lastListSeekIndex
     *
     * @param current
     *            the current key
     * @param fieldName
     *            the current fieldname
     * @param endKey
     *            the end key of the range
     * @param endKeyInclusive
     *            the range end inclusive flag
     * @return the new range to be seek()
     * @see #getWhitelistSeek(Key, String, Key, boolean, List, int) getWhitelistSeek
     */
    private Range getWhitelistSeek(Key current, String fieldName, Key endKey, boolean endKeyInclusive) {
        return getWhitelistSeek(current, fieldName, endKey, endKeyInclusive, sortedWhitelist, lastListSeekIndex);
    }

    /**
     * Moving through the whitelist from the lastHit index create a start key for the next acceptable field/uid
     *
     * @param current
     *            the current key
     * @param fieldName
     *            the current field
     * @param endKey
     *            the range endKey
     * @param endKeyInclusive
     *            the range end inclusive flag
     * @param sortedWhitelist
     *            the sortedWhitelist to use
     * @param lastHit
     *            the starting index to search the whitelist
     * @return the new range can be used to seek to the next key, bypassing irrelevant keys
     */
    private Range getWhitelistSeek(Key current, String fieldName, Key endKey, boolean endKeyInclusive, List<String> sortedWhitelist, int lastHit) {
        Range range = null;

        for (int i = lastHit + 1; i < sortedWhitelist.size(); i++) {
            String nextField = sortedWhitelist.get(i);

            if (fieldName.compareTo(nextField) == 0) {
                // do not generate a seek range if the iterator is still within
                // a query field
                return null;
            } else if (fieldName.compareTo(nextField) < 0) {
                // is the nextField after the current field?
                // seek to this field
                Key startKey = new Key(current.getRow(), current.getColumnFamily(), new Text(nextField + Constants.NULL_BYTE_STRING));
                range = new Range(startKey, true, endKey, endKeyInclusive);
                lastListSeekIndex = i;
                break;
            } else if (i + 1 == sortedWhitelist.size()) {
                // roll to the next uid and reset the lastSeekIndex
                range = getRolloverRange(current, endKey, endKeyInclusive);
                lastListSeekIndex = -1;
                break;
            }
        }

        // none of the fields in the whitelist come after the current field
        if (range == null) {
            // roll to the next uid
            range = getRolloverRange(current, endKey, endKeyInclusive);
            lastListSeekIndex = -1;
        }

        return range;
    }

    /**
     * Get a rollover range that skips to the next child uid
     *
     * @param current
     *            the current key
     * @param end
     *            the end key
     * @param endInclusive
     *            is end key inclusive flag
     * @return a rollover range, or an empty range if the rollover range would extend beyond the end key
     */
    private Range getRolloverRange(Key current, Key end, boolean endInclusive) {

        // ensure this new key won't be beyond the end
        // new CF = current dataType\0uid\0 to ensure the next hit will be in another uid
        Key startKey = current.followingKey(PartialKey.ROW_COLFAM);

        if (startKey.compareTo(end) < 0) {
            // last one, roll over to the first
            return new Range(startKey, true, end, endInclusive);
        }

        // create a range that should have nothing in it
        return getEmptyRange(end, endInclusive);
    }

    /**
     *
     * @param end
     *            the end key
     * @param endInclusive
     *            end inclusive flag
     * @return return an empty range based to be seeked
     */
    protected Range getEmptyRange(Key end, boolean endInclusive) {
        return new Range(end, false, end.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), false);
    }

    private Range getBlacklistSeek(Key current, String fieldName, Key endKey, boolean endKeyInclusive) {
        Range range = null;

        // test for if the seek wrapped to a new uid
        if (lastListSeekIndex > 0 && fieldName.compareTo(sortedBlacklist.get(lastListSeekIndex)) < 0) {
            // reset, the current field is less than the last one
            lastListSeekIndex = -1;
        }

        for (int i = lastListSeekIndex + 1; i < sortedBlacklist.size(); i++) {
            String nextField = sortedBlacklist.get(i);
            int compare = fieldName.compareTo(nextField);
            if (compare == 0) {
                // blacklisted
                Key startKey = new Key(current.getRow(), current.getColumnFamily(), new Text(fieldName + Constants.MAX_UNICODE_STRING));
                if (startKey.compareTo(endKey) < 0) {
                    // seek past the blacklist
                    range = new Range(startKey, false, endKey, endKeyInclusive);
                } else {
                    // seek to the end of the range
                    range = getEmptyRange(endKey, endKeyInclusive);
                }

                // store this to start here next time
                lastListSeekIndex = i;
                // don't keep looking
                break;
            } else if (compare > 0) {
                // update the last seek so this isn't looked at until/unless the document wraps to a new uid
                lastListSeekIndex = i;
            }
        }

        return range;
    }

    /**
     * Bypass the seek we have not met any threshold for seeking
     *
     * @return true if the seek should be bypassed, false otherwise
     */
    protected boolean bypassSeek() {
        return bypassSeekOnMaxFields() && bypassSeekOnMaxKeys();
    }

    /**
     * If maxFieldsBeforeSeek is non-negative, see if the threshold has been met to seek
     *
     * @return true if the seek should be bypassed, false otherwise
     */
    private boolean bypassSeekOnMaxFields() {
        return maxFieldsBeforeSeek == -1 || fieldCount < maxFieldsBeforeSeek;
    }

    /**
     * If maxKeysBeforeSeek is non-negative, see if the threshold has been met to seek
     *
     * @return true if the seek should be bypassed, false otherwise
     */
    private boolean bypassSeekOnMaxKeys() {
        return maxKeysBeforeSeek == -1 || keyMissCount < maxKeysBeforeSeek;
    }

    /**
     * Extract the query fields from the script and sort them
     *
     * @param script
     *            the query script
     * @return a set of identifiers
     */
    private Set<String> extractIdentifiersFromScript(ASTJexlScript script) {
        Set<String> ids = new HashSet<>();
        String field;
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(script);
        for (ASTIdentifier identifier : identifiers) {
            field = JexlASTHelper.deconstructIdentifier(identifier);
            ids.add(field);
        }
        return ids;
    }

    /**
     * Intersect the fields serialized along with the query iterator with the identifiers in the query
     *
     * @param fields
     *            the serialized fields
     * @param script
     *            the query script
     */
    private void setQueryFields(Set<String> fields, ASTJexlScript script) {

        Set<String> identifiers = extractIdentifiersFromScript(script);
        fields = Sets.intersection(fields, identifiers);

        queryFields = Lists.newArrayList(fields);
        Collections.sort(queryFields);
        queryFields = Collections.unmodifiableList(queryFields);
    }

    /**
     * Ensure that if using a whitelist that all the queryFields are on it and that if using a blacklist none of the queryFields are on it. The whitelist and
     * blacklist sets will be modified
     *
     * @param whitelist
     *            the list of whitelist queryFields or null if not using a whitelist
     * @param blacklist
     *            the list of blacklist queryFields or null if not using a blacklist
     */
    private void updateLists(Set<String> whitelist, Set<String> blacklist) {
        if (whitelist != null && !whitelist.isEmpty()) {
            // always add the target queryFields into the whitelist in case it is missing
            for (String field : queryFields) {
                if (!whitelist.contains(field)) {
                    whitelist.add(field);
                }
            }
        }

        if (blacklist != null && !blacklist.isEmpty()) {
            // ensure that none of the required queryFields are on the blacklist
            for (String field : queryFields) {
                if (blacklist.contains(field)) {
                    blacklist.remove(field);
                }
            }
        }
    }

    /**
     * Set the sortedWHitelist and sortedBlacklist from the queryFields modified versions and sort them
     *
     * @param whitelist
     *            the whitelist modified by queryFields
     * @param blacklist
     *            the blacklist modified by queryFields
     */
    private void setSortedLists(Set<String> whitelist, Set<String> blacklist) {
        if (whitelist != null && !whitelist.isEmpty()) {
            sortedWhitelist = new ArrayList<>(whitelist);
            Collections.sort(sortedWhitelist);
            sortedWhitelist = Collections.unmodifiableList(sortedWhitelist);
        }

        if (blacklist != null && !blacklist.isEmpty()) {
            sortedBlacklist = new ArrayList<>(blacklist);
            Collections.sort(sortedBlacklist);
            sortedBlacklist = Collections.unmodifiableList(sortedBlacklist);
        }
    }

    /**
     * Test if a field should be kept and keep state for seeking. Track internal counters for seeking
     *
     * @param current
     *            the current key
     * @param applyCount
     *            true if seeking state should be modified as a result of this call, false otherwise
     * @param isTld
     *            set to true if the key represents a TLD, false otherwise
     * @return true if the key has a field that should be kept, false otherwise
     */
    protected boolean keepField(Key current, boolean applyCount, boolean isTld) {
        String currentField = lastParseInfo.getField();
        if (applyCount) {
            if (currentField.equals(lastField)) {
                // increment counter
                fieldCount++;
            } else {
                // reset the counter
                lastField = currentField;
                fieldCount = 1;
            }
        } else if (!currentField.equals(lastField)) {
            // always update a change in field even if counts aren't applied
            lastField = currentField;
            // since the counts aren't being applied don't increment the count just reset it
            fieldCount = 0;
        }

        boolean keep = keep(currentField, isTld);

        if (applyCount) {
            if (keep) {
                // reset the key counter
                keyMissCount = 0;
            } else {
                keyMissCount++;
            }
        }

        return keep;
    }

    /**
     * Test a field against the whitelist and blacklist
     *
     * @param field
     *            the field to test
     * @param isTld
     *            set to true if the key is from a tld, false otherwise
     * @return true if the field should be kept based on the whitelist/blacklist, false otherwise
     */
    private boolean keep(String field, boolean isTld) {
        if (isFieldLimit(field)) {
            return false;
        }

        if (isTld) {
            if (sortedWhitelist != null) {
                return sortedWhitelist.contains(field);
            } else if (sortedBlacklist != null) {
                return !sortedBlacklist.contains(field);
            } else {
                // neither is specified, keep by default
                return true;
            }
        } else {
            return queryFields.contains(field);
        }
    }

    /**
     * Parse the field from a key. The field will always be stripped of grouping notation since that is how they have been parsed from the original query
     *
     * @param current
     *            the current key
     * @return the field string
     */
    protected String getCurrentField(Key current) {
        ByteSequence cf = current.getColumnFamilyData();

        if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, FI_CF, 0, 2) == 0) {
            ArrayList<Integer> nullIndexes = TLD.instancesOf(0, cf, 1);
            final int startFn = nullIndexes.get(0) + 1;
            final int stopFn = cf.length();

            byte[] fn = new byte[stopFn - startFn];

            System.arraycopy(cf.getBackingArray(), startFn + cf.offset(), fn, 0, stopFn - startFn);

            return JexlASTHelper.deconstructIdentifier(new String(fn));
        } else if (WritableComparator.compareBytes(cf.getBackingArray(), 0, 2, TF_CF, 0, 2) == 0) {
            ByteSequence cq = current.getColumnQualifierData();
            ArrayList<Integer> nullIndexes = TLD.lastInstancesOf(0, cq, 1);
            final int startFn = nullIndexes.get(0) + 1;
            final int stopFn = cq.length();

            byte[] fn = new byte[stopFn - startFn];

            System.arraycopy(cq.getBackingArray(), startFn + cq.offset(), fn, 0, stopFn - startFn);

            return JexlASTHelper.deconstructIdentifier(new String(fn));
        } else {
            final byte[] cq = current.getColumnQualifierData().getBackingArray();
            final int length = cq.length;
            int stopIndex = -1;
            for (int i = 0; i < length - 1; i++) {
                if (cq[i] == 0x00) {
                    stopIndex = i;
                    break;
                } else if (cq[i] == 0x2E) {
                    // test for '.' used in grouping notation
                    stopIndex = i;
                    break;
                }
            }

            return new String(cq, 0, stopIndex);
        }
    }

    /**
     * Test if the field is limited by anyField or specific field limitations and is not a query field
     *
     * @param field
     *            the field to test
     * @return true if the field limit has been reached for this field, false otherwise
     */
    private boolean isFieldLimit(String field) {
        return ((anyFieldLimit != -1 && fieldCount > anyFieldLimit) || (limitFieldsMap.get(field) != null && fieldCount > limitFieldsMap.get(field)))
                        && !queryFields.contains(field);
    }

    /**
     * If the current key is rejected due to a field limit and a field limit field is specified generate a value with the field in it
     *
     * @param key
     *            the key to limit
     * @return a key with the limit field specified, or null if no limit was configured
     */
    @Override
    public Key transform(Key key) {
        if (this.limitFieldsField != null) {
            ParseInfo info = getParseInfo(key);
            if (isFieldLimit(info.getField())) {
                return new Key(key.getRow(), key.getColumnFamily(), new Text(limitFieldsField + Constants.NULL + info.getField()));
            }
        }
        return null;
    }

    @Override
    public EventDataQueryFilter clone() {
        return new TLDEventDataFilter(this);
    }

    /**
     * Place to store all the parsed information about a Key so we don't have to re-parse
     */
    protected static class ParseInfo {
        private Key key;
        private boolean root;
        private String field;

        public ParseInfo(Key k) {
            this.key = k;
        }

        public ParseInfo(ParseInfo other) {
            if (other.key != null) {
                key = new Key(other.key);
            }
            root = other.root;
            field = other.field;
        }

        public boolean isSame(Key other) {
            return this.key.equals(other);
        }

        public boolean isRoot() {
            return root;
        }

        public String getField() {
            return field;
        }

        public void setRoot(boolean root) {
            this.root = root;
        }

        public void setField(String field) {
            this.field = field;
        }
    }
}
