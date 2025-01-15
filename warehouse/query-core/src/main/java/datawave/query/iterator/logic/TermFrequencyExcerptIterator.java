package datawave.query.iterator.logic;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.TermWeight;
import datawave.query.Constants;

/**
 * This iterator is intended to scan the term frequencies for a specified document, field, and offset range. The result will be excerpts for the field specified
 * for each document scanned.
 */
public class TermFrequencyExcerptIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    private static final Logger log = LoggerFactory.getLogger(TermFrequencyExcerptIterator.class);
    private static final Collection<ByteSequence> TERM_FREQUENCY_COLUMN_FAMILY_BYTE_SEQUENCE = Collections
                    .singleton(new ArrayByteSequence(Constants.TERM_FREQUENCY_COLUMN_FAMILY.getBytes()));

    /**
     * A special term that is used to indicate we removed a candidate term because <br>
     * it was a member of the list of terms that should not be included in an excerpt. <br>
     * <br>
     * When returned to the ExcerptTransform, indicates that we should run this iterator a second time with a bigger range so that we can account for removed
     * words in the excerpt.
     */
    public static final String WORD_SKIPPED_MARKER = "XXXWESKIPPEDAWORDXXX";

    /**
     * A special term that is used to indicate when we are using documents that are not scored. <br>
     * <br>
     * When returned to the ExcerptTransform, indicates that we should not populate the "HIT_EXCERPT_WITH_SCORES" or "HIT_EXCERPT_ONE_BEST" fields.
     */
    public static final String NOT_SCORED_MARKER = "XXXNOTSCOREDXXX";

    private static final String BLANK_EXCERPT_MESSAGE = "YOUR EXCERPT WAS BLANK! Maybe bad field or size?";

    /**
     * Encapsulates the configuration of the HitExcerptIterator. The Iterator constructor guarantees one is set per instance of the iterator, method are
     * provided to initialize this from an external options map or perform a deep copy from another instance.
     */
    public static final class Configuration {
        /** The field name option */
        public static final String FIELD_NAME = "field.name";
        /** The start offset option */
        public static final String START_OFFSET = "start.offset";
        /** The end offset option */
        public static final String END_OFFSET = "end.offset";

        /** represents the directions used to build the excerpt in the iterator */
        public enum Direction {
            /** specifies that an excerpt only returns terms prior to the last hit term found in the excerpt */
            BEFORE,
            /** specifies that an excerpt only returns terms after the first hit term found in the excerpt */
            AFTER,
            /** specifies that an excerpt returns terms from both before and after the median offset */
            BOTH
        }

        public Configuration() {}

        /**
         * initialize the fields in this instance by reading from an option map
         *
         * @param options
         *            the map to read options from.
         */
        public void init(Map<String,String> options) {
            fieldName = options.get(FIELD_NAME);
            startOffset = Integer.parseInt(options.get(START_OFFSET));
            endOffset = Integer.parseInt(options.get(END_OFFSET));
            hitTermsList = new ArrayList<>();
            direction = Direction.BOTH;
            origHalfSize = 0;
            trimExcerpt = false;
        }

        /** deep copy the configuration of another excerpt configuration into this one */
        public void deepCopy(Configuration other) {
            fieldName = other.fieldName;
            startOffset = other.startOffset;
            endOffset = other.endOffset;
            hitTermsList = new ArrayList<>(other.hitTermsList);
            direction = other.direction;
            origHalfSize = other.origHalfSize;
            trimExcerpt = other.trimExcerpt;
        }

        /** the field name */
        private String fieldName;

        /** the list of hit terms: terms from the query that resulted in the current document being returned as a result */
        private ArrayList<String> hitTermsList;

        /** the direction for the excerpt - controls which directions we build the excerpt from an originating hit term */
        private Direction direction;

        /**
         * Whether we might need to trim down the excerpt to the requested size. <br>
         * Is false if this is the first time running the iterator and true otherwise because the second pass always asks for more data than is needed to
         * generate the excerpt to account for stop words.
         */
        private boolean trimExcerpt;

        /** the start offset (inclusive) */
        private int startOffset;

        /** the end offset (exclusive) */
        private int endOffset;

        /** The size of half of the original desired excerpt length. Used during trimming. */
        private float origHalfSize;

        public List<String> getHitTermsList() {
            return hitTermsList;
        }

        public String toString() {
            return fieldName + ", " + startOffset + ", " + endOffset;
        }
    }

    /** the underlying source */
    protected SortedKeyValueIterator<Key,Value> source;

    /** The specified dt/uid column families */
    protected SortedSet<String> columnFamilies;

    /** inclusive or exclusive dt/uid column families */
    protected boolean inclusive;

    /** the underlying TF scan range */
    protected Range scanRange;

    /** the top key */
    protected Key tk;

    /** the top value */
    protected Value tv;

    /** encapsulates the configuration for the TermFrequencyExcerptIterator */
    protected final Configuration config;

    public TermFrequencyExcerptIterator() {
        this.config = new Configuration(); // excerpt config will never be null;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = new IteratorOptions(TermFrequencyExcerptIterator.class.getSimpleName(),
                        "An iterator that returns excepts from the scanned documents", null, null);
        options.addNamedOption(Configuration.FIELD_NAME, "The token field name for which to get excerpts (required)");
        options.addNamedOption(Configuration.START_OFFSET, "The start offset for the excerpt (inclusive) (required)");
        options.addNamedOption(Configuration.END_OFFSET, "The end offset for the excerpt (exclusive) (required)");
        return options;
    }

    @Override
    public boolean validateOptions(Map<String,String> map) {
        if (map.containsKey(Configuration.FIELD_NAME)) {
            if (map.get(Configuration.FIELD_NAME).isEmpty()) {
                throw new IllegalArgumentException("Empty field name property: " + Configuration.FIELD_NAME);
            }
        } else {
            throw new IllegalArgumentException("Missing field name property: " + Configuration.FIELD_NAME);
        }

        int startOffset;
        if (map.containsKey(Configuration.START_OFFSET)) {
            try {
                startOffset = Integer.parseInt(map.get(Configuration.START_OFFSET));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse start offset as integer", e);
            }
        } else {
            throw new IllegalArgumentException("Missing start offset property: " + Configuration.START_OFFSET);
        }

        if (map.containsKey(Configuration.END_OFFSET)) {
            int endOffset;
            try {
                endOffset = Integer.parseInt(map.get(Configuration.END_OFFSET));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse end offset as integer", e);
            }
            if (endOffset <= startOffset) {
                throw new IllegalArgumentException("End offset must be greater than start offset");
            }
        } else {
            throw new IllegalArgumentException("Missing end offset property: " + Configuration.END_OFFSET);
        }

        return true;
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        TermFrequencyExcerptIterator it = new TermFrequencyExcerptIterator();
        it.source = source.deepCopy(env);
        it.config.deepCopy(config);
        return it;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        config.init(options);
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("{} seek'ing with requested range {}", this, range);
        }

        // capture the column families and the inclusiveness
        this.columnFamilies = columnFamilies != null ? getSortedCFs(columnFamilies) : Collections.emptySortedSet();
        this.inclusive = inclusive;

        // Determine the start key in the term frequencies
        Key startKey = null;
        if (range.getStartKey() != null) {
            // get the start document
            String dtAndUid = getDtUidFromEventKey(range.getStartKey(), true, range.isStartKeyInclusive());
            // if no start document
            if (dtAndUid == null) {
                // if no column families or not using these column families inclusively
                if (this.columnFamilies.isEmpty() || !this.inclusive) {
                    // then start at the beginning of the tf range
                    startKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY);
                } else {
                    // otherwise start at the first document specified
                    startKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY,
                                    new Text(this.columnFamilies.first() + Constants.NULL));
                }
            } else {
                // we had a start document specified in the start key, so start there
                startKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(dtAndUid));
            }
        }
        log.debug("{} calling seek to start key: {}", this, startKey);

        // Determine the end key in the term frequencies
        Key endKey = null;
        if (range.getEndKey() != null) {
            // get the end document
            String dtAndUid = getDtUidFromEventKey(range.getEndKey(), false, range.isEndKeyInclusive());
            // if no end document
            if (dtAndUid == null) {
                // if we do not have column families specified, or they are not inclusive
                if (this.columnFamilies.isEmpty() || !this.inclusive) {
                    // then go to the end of the TFs
                    endKey = new Key(range.getEndKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(Constants.MAX_UNICODE_STRING));
                } else {
                    // otherwise end at the last document specified
                    endKey = new Key(range.getEndKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY,
                                    new Text(this.columnFamilies.last() + Constants.NULL + Constants.MAX_UNICODE_STRING));
                }
            } else {
                // we had an end document specified in the end key, so end there
                endKey = new Key(range.getStartKey().getRow(), Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(dtAndUid));
            }
        }
        log.debug("{} seek'ing to end key: {}", this, endKey);

        // if we have actually exhausted our range, then return with no next key
        if (endKey != null && startKey != null && endKey.compareTo(startKey) <= 0) {
            this.scanRange = null;
            this.tk = null;
            this.tv = null;
            return;
        }

        // set our term frequency scan range
        this.scanRange = new Range(startKey, false, endKey, false);

        if (log.isDebugEnabled()) {
            log.debug("{} seek'ing to: {} from requested range {}", this, this.scanRange, range);
        }

        // seek the underlying source
        source.seek(this.scanRange, TERM_FREQUENCY_COLUMN_FAMILY_BYTE_SEQUENCE, true);

        // get the next key
        next();
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;

        if (log.isTraceEnabled()) {
            log.trace("{} calling next on {}", source.hasTop(), scanRange);
        }

        // find a valid dt/uid (depends on initial column families set in seek call)
        String dtUid = null;
        while (source.hasTop() && dtUid == null) {
            Key top = source.getTopKey();
            String thisDtUid = getDtUidFromTfKey(top);
            // if this dt and uid are in the accepted column families...
            if (columnFamilies.contains(thisDtUid) == inclusive) {
                // we can use this document
                dtUid = thisDtUid;
            } else {
                seekToNextUid(top.getRow(), thisDtUid);
            }
        }

        // if no more term frequencies, then we are done.
        if (!source.hasTop() || dtUid == null) {
            return;
        }

        final int startOffset = config.startOffset;
        final int endOffset = config.endOffset;
        final List<String> hitTermsList = config.hitTermsList;
        final String fieldName = config.fieldName;

        Key top = source.getTopKey();

        // set the size of the array to the number of offsets that we will try to fill for the potential excerpt
        WordsAndScores[] wordsAndScoresArr = new WordsAndScores[endOffset - startOffset];

        // while we have term frequencies for the same document
        while (source.hasTop() && dtUid.equals(getDtUidFromTfKey(source.getTopKey()))) {
            top = source.getTopKey();

            // get the field and value
            String[] fieldAndValue = getFieldAndValue(top);

            // if this is for the field we are summarizing
            if (fieldName.equals(fieldAndValue[0])) {
                try {
                    // get the protobuf that contains all the extra information for the TFs from the value
                    TermWeight.Info info = TermWeight.Info.parseFrom(source.getTopValue().get());

                    // check if the number of scores is equal to the number of offsets
                    boolean useScores = info.getScoreCount() == info.getTermOffsetCount();
                    List<Integer> scoreList = null;
                    // if the number of scores and offsets is the same, check to see if all the scores are negative or not
                    if (useScores) {
                        scoreList = info.getScoreList();
                        useScores = !hasOnlyNegativeScores(scoreList, info, startOffset, endOffset);
                    }

                    // for each offset, gather all the terms in our range
                    for (int i = 0, termOffsetCount = info.getTermOffsetCount(); i < termOffsetCount; i++) {
                        int offset = info.getTermOffset(i);
                        // if the offset is within our range
                        if (offset >= startOffset && offset < endOffset) {
                            // calculate the index in our value list
                            int index = offset - startOffset;
                            // if the current index has no words/scores yet, initialize an object at the index
                            if (wordsAndScoresArr[index] == null) {
                                wordsAndScoresArr[index] = new WordsAndScores();
                            }
                            boolean stopFound;
                            // if we are using scores, add the word and score to the object, if not then only add the word
                            if (useScores) {
                                stopFound = wordsAndScoresArr[index].addTerm(fieldAndValue[1], scoreList.get(i), hitTermsList);
                            } else {
                                stopFound = wordsAndScoresArr[index].addTerm(fieldAndValue[1], hitTermsList);
                            }
                            // if we encounter a stop word, and we're not in trim mode, fail-fast and create an entry
                            // to return the special marker token. When seeing this, the transform will run this again
                            // in trim mode with an expanded offset range.
                            if (stopFound && !config.trimExcerpt) {
                                tk = new Key(top.getRow(), new Text(dtUid), new Text(fieldName + Constants.NULL + WORD_SKIPPED_MARKER + Constants.NULL
                                                + WORD_SKIPPED_MARKER + Constants.NULL + WORD_SKIPPED_MARKER), top.getColumnVisibility(), top.getTimestamp());
                                tv = new Value();
                                return;
                            }
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.warn("Value found in tf column was not a valid TermWeight.Info, skipping", e);
                }
            }
            // get the next term frequency
            source.next();
        }
        // Now that the words and scores array is populated with all the tf data, it's time to generate an excerpt and
        // create a key that contains all of our excerpts to be read by the ExcerptTransform
        tk = new Key(top.getRow(), new Text(dtUid), new Text(fieldName + Constants.NULL + generateExcerpt(wordsAndScoresArr)), top.getColumnVisibility(),
                        top.getTimestamp());
        tv = new Value();
    }

    /** Checks whether the passed in list has only negative scores or not within the iterator range. */
    private static boolean hasOnlyNegativeScores(List<Integer> scoreList, TermWeight.Info info, int startOffset, int endOffset) {
        // check each score and if it is positive and within the offsets we are looking at, return false
        for (int i = 0, scoreListSize = scoreList.size(); i < scoreListSize; i++) {
            int offset = info.getTermOffset(i);
            if (scoreList.get(i) >= 0 && offset >= startOffset && offset < endOffset) {
                return false;
            }
        }
        // we have not found a positive number within our offsets, return true
        return true;
    }

    /**
     * Generates multiple variations of excerpts and creates the top key and value containing them for return to the transform. In this method, we'll generate
     * the following excerpts:
     * <ul>
     * <li>phraseWithScoresExcerpt</li>
     * <li>phraseWithoutScoresExcerpt</li>
     * <li>oneBestExcerpt</li>
     * </ul>
     * These get packaged into the key returned to the transform. Ultimately the transform will decide which one we use.
     *
     * @param wordsAndScoresArr
     *            a collection of document terms and their scores, organized by offset. Each offset may have multiple terms to choose from. Some offsets may be
     *            null if there were no tf's from that position.
     */
    protected String generateExcerpt(WordsAndScores[] wordsAndScoresArr) {

        boolean usedScores = false;
        String phraseWithScoresExcerpt = null;
        String oneBestExcerpt = null;

        // loop through the WordsAndScores and if we find at least one that has scores, generate a phrase with scores based excerpt
        for (WordsAndScores wordsAndScores : wordsAndScoresArr) {
            if (wordsAndScores != null && wordsAndScores.getUseScores()) {
                phraseWithScoresExcerpt = generatePhrase(wordsAndScoresArr, config);
                usedScores = true;
                break;
            }
        }

        // if we did not find any scores in the entire wordsAndScoresArr, add a marker.
        if (!usedScores) {
            phraseWithScoresExcerpt = NOT_SCORED_MARKER;
            oneBestExcerpt = NOT_SCORED_MARKER;
        } else { // if we have any scores, set all output scores flags to false.
            for (WordsAndScores wordsAndScores : wordsAndScoresArr) {
                if (wordsAndScores != null && wordsAndScores.getUseScores()) {
                    wordsAndScores.setOutputScores(false);
                }
            }
        }

        // Generate the "phrase without scores" excerpt now that the scores flags are false.
        String phraseWithoutScoresExcerpt = generatePhrase(wordsAndScoresArr, config);

        // if the regular excerpt is blank, we will return a message saying that the excerpt was blank
        if (phraseWithoutScoresExcerpt.isBlank()) {
            phraseWithoutScoresExcerpt = BLANK_EXCERPT_MESSAGE;
            if (usedScores) {
                phraseWithScoresExcerpt = BLANK_EXCERPT_MESSAGE;
                oneBestExcerpt = BLANK_EXCERPT_MESSAGE;
            }
        } else {
            if (usedScores) {
                // prepare all the WordsAndScores to output for the "one best" excerpt
                for (WordsAndScores wordsAndScores : wordsAndScoresArr) {
                    if (wordsAndScores != null) {
                        wordsAndScores.setOneBestExcerpt(true);
                    }
                }
                // generate the "one best" excerpt
                oneBestExcerpt = generatePhrase(wordsAndScoresArr, config);
            }
        }
        // return all the excerpt sections concatenated with nulls between them
        return phraseWithScoresExcerpt + Constants.NULL + phraseWithoutScoresExcerpt + Constants.NULL + oneBestExcerpt;
    }

    /**
     * Generate a phrase from the given lists of WordsAndScores
     *
     * @param wordsAndScoresArr
     *            the array of WordsAndScores that contain the terms to create a phrase from
     * @param config
     *            the configuration for the ExcerptIterator
     * @return the phrase
     */
    private static String generatePhrase(WordsAndScores[] wordsAndScoresArr, final Configuration config) {
        // put brackets around whole hit phrases instead of individual terms
        checkForHitPhrase(wordsAndScoresArr, config.hitTermsList);

        // there are cases where we'll have no scores, and we will want to choose a longer term for a particular position
        // instead of the hit term we matched (e.g., when we're dealing with synonyms that are a fragment of a larger
        // term.) This method will set an override for that position so that we'll choose the longer term instead of
        // a shorter synonym, which is typically a substring of the longer term.
        overrideOutputLongest(wordsAndScoresArr);

        // create an array with the same length as the one we just passed in
        String[] termsToOutput = new String[wordsAndScoresArr.length];

        // pull our items out of the care package
        Configuration.Direction direction = config.direction;
        boolean trimExcerpt = config.trimExcerpt;
        int startOffset = config.startOffset;
        int endOffset = config.endOffset;
        float origHalfSize = config.origHalfSize;

        boolean bef = direction.equals(Configuration.Direction.BEFORE);
        boolean aft = direction.equals(Configuration.Direction.AFTER);

        // tracks whether we've found the first hit term
        boolean firstHitTermFound = false;

        // tracks index of the first hit term found in the words array
        int beforeIndex = -1;
        // tracks index of the last hit term found in the words array.
        int afterIndex = -1;

        int debugCounter = 0; // FOR DEBUG: counter used for debug logging

        // go through the whole WordsAndScoresArr and try to get a word to output for each offset
        for (int i = 0; i < wordsAndScoresArr.length; i++) {
            // if there is nothing at this position, put nothing at the position in the output
            if (wordsAndScoresArr[i] == null) {
                termsToOutput[i] = null;
                continue;
            }

            termsToOutput[i] = wordsAndScoresArr[i].getWordToOutput();
            // If the WordsAndScores returned null, that means the word chosen for this position is something we do not want to output.
            if (termsToOutput[i] != null) {
                // if the user has requested BEFORE or AFTER and this object had a hit term...
                if ((bef || aft) && (wordsAndScoresArr[i].getHasHitTerm())) {
                    // if this is the first hit term for the excerpt, set this offset as the "afterIndex" and set the lock so we do not write over it
                    if (!firstHitTermFound) {
                        afterIndex = i;
                        firstHitTermFound = true;
                    }
                    // set this offset as the "beforeIndex" (no lock on this one because we want it to keep being overwritten with the last hit term offset)
                    beforeIndex = i;
                }
            }

            // FOR DEBUG:--------------------------------------------------------------------------------------------------------------
            else { // counting how many things we don't want to output have been removed from the excerpt
                if (log.isDebugEnabled() && trimExcerpt) {
                    debugCounter++;
                }
            }
            // FOR DEBUG:--------------------------------------------------------------------------------------------------------------
        }

        // FOR DEBUG:----------------------------------------------------------------------------------------------------------------------
        if (log.isDebugEnabled() && trimExcerpt) {
            log.debug("{} words removed from expanded range ({},{})", debugCounter, startOffset, endOffset);
            debugCounter = 0;
            // counting how many words we have in the excerpt (after removing stop words) before any trimming is done
            for (String s : termsToOutput) {
                if (s != null) {
                    debugCounter++;
                }
            }
            log.debug("{} words in excerpt before trimming (we want this to be greater than or equal to \"size of excerpt requested\" from ExcerptTransform)",
                            debugCounter);
        }
        // -------------------------------------------------------------------------------------------------------------------------------

        // if no BEFORE or AFTER AND a hit term wasn't found...
        if (!firstHitTermFound) {
            // join everything together with spaces while skipping null offsets (and trimming if we need to)
            return !trimExcerpt ? joinExcerpt(termsToOutput) : joinExcerpt(bothTrim(termsToOutput, origHalfSize, (endOffset - startOffset) / 2));
        } else {
            if (bef) { // if direction is "before", set everything after the last hit term to null
                for (int k = beforeIndex + 1; k < wordsAndScoresArr.length; k++) {
                    termsToOutput[k] = null;
                }
                // trim the excerpt down if we need
                if (trimExcerpt) {
                    int start = (int) (beforeIndex - (origHalfSize * 2));
                    trimBeginning(termsToOutput, beforeIndex, start);
                }
            } else { // if direction is "after", set everything before the first hit term to null
                for (int k = 0; k < afterIndex; k++) {
                    termsToOutput[k] = null;
                }
                // trim the excerpt down if we need
                if (trimExcerpt) {
                    int start = (int) (afterIndex + (origHalfSize * 2));
                    trimEnd(termsToOutput, afterIndex, start);
                }
            }
            // join everything together with spaces while skipping null offsets
            return joinExcerpt(termsToOutput);
        }
    }

    /**
     * Joins together all the individual terms from the passed in array while skipping any null values.
     *
     * @param termsToOutput
     *            the terms to create a phrase from
     * @return the finalized joined together string (the excerpt)
     */
    private static String joinExcerpt(String[] termsToOutput) {
        StringJoiner j = new StringJoiner(" ");
        for (String s : termsToOutput) {
            if (s != null) {
                j.add(s);
            }
        }
        return j.toString();
    }

    /**
     * Trim down both side of the excerpt to the size that we want
     *
     * @param termsToOutput
     *            the terms to create a phrase from
     * @param origHalfSize
     *            The size of half of the original desired excerpt length. Used during trimming.
     * @param expandedMid
     *            Calculated by <code>(endOffset - startOffset) / 2</code>. Is the midpoint of the expanded range.
     * @return the trimmed array
     */
    private static String[] bothTrim(String[] termsToOutput, float origHalfSize, int expandedMid) {
        // calculate the midpoint of the expanded start and end offsets (because this should only be triggered on a second attempt)
        int start = (int) (expandedMid - origHalfSize);
        // trim the beginning down to size
        trimBeginning(termsToOutput, expandedMid, start);
        start = (int) (expandedMid + origHalfSize);
        // trim the end down to size
        trimEnd(termsToOutput, expandedMid, start);
        return termsToOutput;
    }

    /**
     * Trims off the front to get the correct size
     *
     * @param termsToOutput
     *            the terms to create a phrase from
     * @param beforeIndex
     *            the index in the array to start at
     * @param start
     *            the index in the array where we want to start setting values to null at
     */
    private static void trimBeginning(String[] termsToOutput, int beforeIndex, int start) {
        boolean startNull = false;
        // start at "beforeIndex" and work our way backwards through the array
        while (beforeIndex >= 0) {
            // if we have not started the trimming yet, but we are at the index were we need to start...
            if (!startNull && beforeIndex < start) {
                startNull = true;
            }
            // if we have passed the offset where we should be trimming, set this index to null
            if (startNull) {
                termsToOutput[beforeIndex] = null;
            } else {
                // if we have not started trimming and this index is null, decrement "start" so that we can use an extra offset to get the excerpt size we want
                if (termsToOutput[beforeIndex] == null) {
                    start--;
                }
            }
            beforeIndex--;
        }
    }

    /**
     * Trims off the end to get the correct size
     *
     * @param termsToOutput
     *            the terms to create a phrase from
     * @param afterIndex
     *            the index in the array to start at
     * @param start
     *            the index in the array where we want to start setting values to null at
     */
    private static void trimEnd(String[] termsToOutput, int afterIndex, int start) {
        boolean startNull = false;
        // start at "afterIndex" and work our way through the array
        while (afterIndex < termsToOutput.length) {
            // if we have not started the trimming yet, but we are at the index were we need to start...
            if (!startNull && afterIndex > start) {
                startNull = true;
            }
            // if we have passed the offset where we should be trimming, set this index to null
            if (startNull) {
                termsToOutput[afterIndex] = null;
            } else {
                // if we have not started trimming and this index is null, increment "start" so that we can use an extra offset to get the excerpt size we want
                if (termsToOutput[afterIndex] == null) {
                    start++;
                }
            }
            afterIndex++;
        }
    }

    /**
     * Looks for hit phrases (not separate hit terms) and puts the whole phrase in brackets
     *
     * @param wordsAndScoresArr
     *            the terms to create a phrase from
     * @param hitTermsList
     *            the list of all hit terms
     */
    private static void checkForHitPhrase(WordsAndScores[] wordsAndScoresArr, List<String> hitTermsList) {
        ArrayList<String> hitPhrases = new ArrayList<>();
        // checks for phrases (anything in the hit list with a space in it) and adds them to a new arrayList
        for (String s : hitTermsList) {
            if (s.contains(" ")) {
                hitPhrases.add(s);
            }
        }
        // if we don't find any, return unchanged
        if (hitPhrases.isEmpty()) {
            return;
        }

        // for each hit phrase found...
        for (String hitPhrase : hitPhrases) {
            // split the phrase on the spaces into the separate terms
            String[] individualHitTerms = hitPhrase.split(" ");
            // if the phrase is almost the same size as the whole excerpt, skip this iteration
            if ((wordsAndScoresArr.length - 2) < individualHitTerms.length) {
                continue;
            }
            // iterate across the WordsAndScores until the end of the hit phrase reaches the last offset
            int iterations = wordsAndScoresArr.length - individualHitTerms.length + 1;
            for (int j = 0; j < iterations; j++) {
                // if we find the hit phrase...
                if (isPhraseFound(individualHitTerms, wordsAndScoresArr, j)) {
                    // set which position in the phrase each offset is
                    for (int k = 0; k < individualHitTerms.length; k++) {
                        int overridePosition;
                        // beginning of phrase
                        if (k == 0) {
                            overridePosition = 1;
                        } else if (k == individualHitTerms.length - 1) { // end of phrase
                            overridePosition = 3;
                        } else { // middle of phrase
                            overridePosition = 2;
                        }
                        // set the override values for the current positions WordsAndScores to the index of the hit term in this position plus the override
                        wordsAndScoresArr[j + k].setOverride(wordsAndScoresArr[j + k].getWordsList().indexOf(individualHitTerms[k]), overridePosition);
                    }
                }
            }
        }
    }

    /**
     * Check to see if the whole hit phrase is found in the offsets starting at the passed in j value
     *
     * @param individualHitTerms
     *            the array of the hit phrase split into individual terms
     *
     * @param terms
     *            the terms to create a phrase from
     *
     * @param j
     *            the current starting offset in the WordsAndScores array
     * @return boolean isPhraseFound
     */
    private static boolean isPhraseFound(String[] individualHitTerms, WordsAndScores[] terms, int j) {
        // k represents what position we are in of the individual hit terms array
        for (int k = 0; k < individualHitTerms.length; k++) {
            // if a WordsAndScores is null, the phrase obviously wasn't found
            if (terms[j + k] == null) {
                return false;
            }
            // get the words list from the current WordsAndScores
            ArrayList<String> tempWords = (ArrayList<String>) terms[j + k].getWordsList();
            // if the current WordsAndScores doesn't have the term for this position, the phrase obviously wasn't found
            if (!tempWords.contains(individualHitTerms[k])) {
                return false;
            }
        }
        // we found the whole phrase!!!
        return true;
    }

    /**
     * If scores are not used, checks to see if a WordsAndScores object has a hit term in it and if so, sees if the hit word is also the longest word. If not,
     * then it will set an override so that the longest word is output with brackets. This is useful in the case where we are dealing with synonyms, where there
     * will be a single long word in the same position as fragments of that word, and one of the fragments is the hit term
     *
     * @param wordsAndScoresArr
     *            the terms to create a phrase from
     */
    private static void overrideOutputLongest(WordsAndScores[] wordsAndScoresArr) {
        // check to see if we find scores and if so, return unchanged
        for (WordsAndScores ws : wordsAndScoresArr) {
            if (ws != null && ws.getUseScores()) {
                return;
            }
        }
        // we now know that scores are not being used for this excerpt
        for (WordsAndScores ws : wordsAndScoresArr) {
            // if a WordsAndScores is not null and has a hit term in it...
            if (ws != null && ws.getHasHitTerm()) {
                // get the index of its longest word
                int lwi = ws.getLongestWordIndex();
                // check to see if the index of its longest word is not the same as the index of the hit term (meaning that the hit term is NOT the longest
                // word)
                if (ws.getHitTermIndex() != lwi) {
                    // get the override value from the WordsAndScores
                    int ov = ws.getOverrideValue();
                    // if this WordsAndScores has a valid override, use the same value but set the index to the index of the longest word
                    // if it does not have a valid override yet, set it with the index of the longest word and an override value of "4"
                    ws.setOverride(lwi, ov >= 0 ? ov : 4);
                }
            }
        }
    }

    /**
     * Seek to the dt/uid following the one passed in
     *
     * @param row
     *            a row
     * @param dtAndUid
     *            the dt and uid string
     * @throws IOException
     *             for issues with read/write
     */
    private void seekToNextUid(Text row, String dtAndUid) throws IOException {
        Key startKey = new Key(row, Constants.TERM_FREQUENCY_COLUMN_FAMILY, new Text(dtAndUid + '.'));
        this.scanRange = new Range(startKey, false, this.scanRange.getEndKey(), this.scanRange.isEndKeyInclusive());
        if (log.isDebugEnabled()) {
            log.debug("{} seek'ing to next document: {}", this, this.scanRange);
        }

        source.seek(this.scanRange, Collections.singleton(new ArrayByteSequence(Constants.TERM_FREQUENCY_COLUMN_FAMILY.getBytes())), true);
    }

    /**
     * Turn a set of column families into a sorted string set
     *
     * @param columnFamilies
     *            the column families
     * @return a sorted set of column families as Strings
     */
    private static SortedSet<String> getSortedCFs(Collection<ByteSequence> columnFamilies) {
        return columnFamilies.stream().map(m -> {
            try {
                return Text.decode(m.getBackingArray(), m.offset(), m.length());
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Get the field and value from the end of the column qualifier of the tf key
     *
     * @param tfKey
     *            the term freq key
     * @return the field name
     */
    private static String[] getFieldAndValue(Key tfKey) {
        String cq = tfKey.getColumnQualifier().toString();
        int index = cq.lastIndexOf(Constants.NULL);
        String fieldName = cq.substring(index + 1);
        int index2 = cq.lastIndexOf(Constants.NULL, index - 1);
        String fieldValue = cq.substring(index2 + 1, index);
        return new String[] {fieldName, fieldValue};
    }

    /**
     * get the dt and uid from a tf key
     *
     * @param tfKey
     *            the term freq key
     * @return the dt\x00uid
     */
    private static String getDtUidFromTfKey(Key tfKey) {
        return getDtUid(tfKey.getColumnQualifier().toString());
    }

    /**
     * Get the dt and uid start or end given an event key
     *
     * @param eventKey
     *            an event key
     * @param startKey
     *            a start key
     * @param inclusive
     *            inclusive boolean flag
     * @return the start or end document (cq) for our tf scan range. Null if dt,uid does not exist in the event key
     */
    private static String getDtUidFromEventKey(Key eventKey, boolean startKey, boolean inclusive) {
        // if an infinite end range, or unspecified end document, then no document to specify
        if (eventKey == null || eventKey.getColumnFamily() == null || eventKey.getColumnFamily().getLength() == 0) {
            return null;
        }

        // get the dt/uid from the cf
        String cf = eventKey.getColumnFamily().toString();
        String dtAndUid = getDtUid(cf);

        // if calculating a start cq
        if (startKey) {
            // if the start dt/uid is inclusive and the cf is only the dt and uid, then include this document,
            // otherwise start at the next document
            return inclusive && cf.equals(dtAndUid) ? dtAndUid + Constants.NULL : dtAndUid + Constants.ONE_BYTE;
        }
        // if calculating an end cq
        else {
            // if the end dt/uid is inclusive or the cf was not only the dt and uid, then include this document,
            // otherwise stop before this document
            return inclusive || !cf.equals(dtAndUid) ? dtAndUid + Constants.NULL + Constants.MAX_UNICODE_STRING : dtAndUid + Constants.NULL;
        }
    }

    // get the dt/uid from the beginning of a given string
    private static String getDtUid(String str) {
        int index = str.indexOf(Constants.NULL);
        index = str.indexOf(Constants.NULL, index + 1);
        return index == -1 ? str : str.substring(0, index);
    }

    public void setHitTermsList(List<String> hitTermsList) {
        this.config.hitTermsList = (ArrayList<String>) hitTermsList;
    }

    public void setDirection(String direction) {
        this.config.direction = Configuration.Direction.valueOf(direction.toUpperCase());
    }

    public void setOrigHalfSize(float origHalfSize) {
        this.config.origHalfSize = origHalfSize;
    }

    public void setTrimExcerpt(boolean trimExcerpt) {
        this.config.trimExcerpt = trimExcerpt;
    }

    @Override
    public String toString() {
        return "TermFrequencyExcerptIterator: " + config;
    }

}
