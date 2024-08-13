package datawave.query.iterator.logic;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
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

import com.google.common.base.Joiner;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.TermWeight;
import datawave.query.Constants;

/**
 * This iterator is intended to scan the term frequencies for a specified document, field, and offset range. The result will be excerpts for the field specified
 * for each document scanned.
 */
public class TermFrequencyExcerptIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    private static final Logger log = LoggerFactory.getLogger(TermFrequencyExcerptIterator.class);
    private static final Joiner joiner = Joiner.on(" ").skipNulls();

    /** The field name option */
    public static final String FIELD_NAME = "field.name";
    /** The start offset option */
    public static final String START_OFFSET = "start.offset";
    /** The end offset option */
    public static final String END_OFFSET = "end.offset";

    /** the underlying source */
    protected SortedKeyValueIterator<Key,Value> source;
    /** the field name */
    protected String fieldName;
    /** the start offset (inclusive) */
    protected int startOffset;
    /** the end offset (exclusive) */
    protected int endOffset;

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

    /** the list of hit terms: terms from the query that resulted in the current document being returned as a result */
    protected ArrayList<String> hitTermsList;
    /** the direction for the excerpt - controls which directions we build the excerpt from an originating hit term */
    private String direction;
    /** specifies that an excerpt is built using the terms prior to the hit term(s) */
    private static final String BEFORE = "BEFORE";
    /** specifies that an excerpt is built using the terms after the hit term(s) */
    private static final String AFTER = "AFTER";
    /** specifies that an excerpt is built using the terms that appear on both sides of the hit terms(s) */
    private static final String BOTH = "BOTH";

    /**
     * Whether we might need to trim down the excerpt to the requested size. <br>
     * Is false if this is the first time running the iterator and true otherwise.
     */
    private boolean trim;
    /** The size of half of the original desired excerpt length. Used during trimming. */
    private float origHalfSize;

    /**
     * A special term that is used to indicate we removed a candidate term because <br>
     * it was a member of the list of terms that should not be included in an excerpt.
     */
    private static final String XXXWESKIPPEDAWORDXXX = "XXXWESKIPPEDAWORDXXX";

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = new IteratorOptions(TermFrequencyExcerptIterator.class.getSimpleName(),
                        "An iterator that returns excepts from the scanned documents", null, null);
        options.addNamedOption(FIELD_NAME, "The token field name for which to get excerpts (required)");
        options.addNamedOption(START_OFFSET, "The start offset for the excerpt (inclusive) (required)");
        options.addNamedOption(END_OFFSET, "The end offset for the excerpt (exclusive) (required)");
        return options;
    }

    @Override
    public boolean validateOptions(Map<String,String> map) {
        if (map.containsKey(FIELD_NAME)) {
            if (map.get(FIELD_NAME).isEmpty()) {
                throw new IllegalArgumentException("Empty field name property: " + FIELD_NAME);
            }
        } else {
            throw new IllegalArgumentException("Missing field name property: " + FIELD_NAME);
        }

        int startOffset;
        if (map.containsKey(START_OFFSET)) {
            try {
                startOffset = Integer.parseInt(map.get(START_OFFSET));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse start offset as integer", e);
            }
        } else {
            throw new IllegalArgumentException("Missing start offset property: " + START_OFFSET);
        }

        int endOffset;
        if (map.containsKey(END_OFFSET)) {
            try {
                endOffset = Integer.parseInt(map.get(END_OFFSET));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse end offset as integer", e);
            }
            if (endOffset <= startOffset) {
                throw new IllegalArgumentException("End offset must be greater than start offset");
            }
        } else {
            throw new IllegalArgumentException("Missing end offset property: " + END_OFFSET);
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
        it.startOffset = startOffset;
        it.endOffset = endOffset;
        it.fieldName = fieldName;
        it.hitTermsList = hitTermsList;
        it.direction = direction;
        it.origHalfSize = origHalfSize;
        it.trim = trim;
        return it;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.startOffset = Integer.parseInt(options.get(START_OFFSET));
        this.endOffset = Integer.parseInt(options.get(END_OFFSET));
        this.fieldName = options.get(FIELD_NAME);
        hitTermsList = new ArrayList<>();
        direction = BOTH;
        origHalfSize = 0;
        trim = false;
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
        log.debug("{} seek'ing with requested range {}", this, range);

        // capture the column families and the inclusiveness
        if (columnFamilies != null) {
            this.columnFamilies = getSortedCFs(columnFamilies);
        } else {
            this.columnFamilies = Collections.emptySortedSet();
        }
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
        log.debug("{} seek'ing to start key: {}", this, startKey);

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
        source.seek(this.scanRange, Collections.singleton(new ArrayByteSequence(Constants.TERM_FREQUENCY_COLUMN_FAMILY.getBytes())), true);

        // get the next key
        next();
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;

        if (log.isTraceEnabled()) {
            log.trace("{} next'ing on {}", source.hasTop(), scanRange);
        }

        // find a valid dt/uid (depends on initial column families set in seek call)
        String dtUid = null;
        while (source.hasTop() && dtUid == null) {
            Key top = source.getTopKey();
            String thisDtUid = getDtUidFromTfKey(top);
            if (isUsableDocument(thisDtUid)) {
                dtUid = thisDtUid;
            } else {
                seekToNextUid(top.getRow(), thisDtUid);
            }
        }

        // if no more term frequencies, then we are done.
        if (!source.hasTop()) {
            return;
        }

        // get the pieces from the top key that will be returned
        Key top = source.getTopKey();
        Text cv = top.getColumnVisibility();
        long ts = top.getTimestamp();
        Text row = top.getRow();
        // set the size of the array to the amount of terms we need to choose
        WordsAndScores[] wordsAndScoresArr = new WordsAndScores[endOffset - startOffset];
        // all of these variables are used in the loops so we'll just keep reassigning instead of creating a bunch of new ones every time
        // make sure that these are reassigned before being accessed in each iteration
        boolean stopFound;
        boolean useScores;
        int tmpOffset;
        int tmpIndex;
        List<Integer> scoreList;

        if (dtUid == null) {
            return;
        }

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
                    useScores = info.getScoreCount() == info.getTermOffsetCount();
                    scoreList = null;
                    // if the number of scores and offsets is the same, check to see if all the scores are negative or not
                    if (useScores) {
                        scoreList = info.getScoreList();
                        useScores = !hasOnlyNegativeScores(scoreList, info);
                    }

                    // for each offset, gather all the terms in our range
                    for (int i = 0, termOffsetCount = info.getTermOffsetCount(); i < termOffsetCount; i++) {
                        tmpOffset = info.getTermOffset(i);
                        // if the offset is within our range
                        if (tmpOffset >= startOffset && tmpOffset < endOffset) {
                            // calculate the index in our value list
                            tmpIndex = tmpOffset - startOffset;
                            // if the current index has no words/scores yet, initialize an object at the index
                            if (wordsAndScoresArr[tmpIndex] == null) {
                                wordsAndScoresArr[tmpIndex] = new WordsAndScores();
                            }
                            // if we are using scores, add the word and score to the object, if not then only add the word
                            if (useScores) {
                                stopFound = wordsAndScoresArr[tmpIndex].addTerm(fieldAndValue[1], scoreList.get(i), hitTermsList);
                            } else {
                                stopFound = wordsAndScoresArr[tmpIndex].addTerm(fieldAndValue[1], hitTermsList);
                            }
                            // this is the fast-fail: if we find a stop word, and we are on the first attempt, exit out so that ExcerptTransform can run this
                            // again with an expanded range
                            if (stopFound && !trim) {
                                tk = new Key(row, new Text(dtUid), new Text(fieldName + Constants.NULL + XXXWESKIPPEDAWORDXXX + Constants.NULL
                                                + XXXWESKIPPEDAWORDXXX + Constants.NULL + XXXWESKIPPEDAWORDXXX), cv, ts);
                                tv = new Value();
                                return;
                            }
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error("Value found in tf column was not of type TermWeight.Info, skipping", e);
                }
            }
            // get the next term frequency
            source.next();
        }
        // generate the return key and value
        generateExcerpt(wordsAndScoresArr, dtUid, cv, ts, row);
    }

    /** Checks whether the passed in list has only negative scores or not within the iterator range. */
    private boolean hasOnlyNegativeScores(List<Integer> scoreList, TermWeight.Info info) {
        int tmpOffset;
        // check each score and if it is positive and within the offsets we are looking at, return false
        for (int i = 0, scoreListSize = scoreList.size(); i < scoreListSize; i++) {
            tmpOffset = info.getTermOffset(i);
            if (scoreList.get(i) >= 0 && tmpOffset >= startOffset && tmpOffset < endOffset) {
                return false;
            }
        }
        // we have not found a positive number within our offsets, return true
        return true;
    }

    /** Generates the differently formatted excerpts and creates the top key and value containing them. */
    private void generateExcerpt(WordsAndScores[] wordsAndScoresArr, String dtUid, Text cv, long ts, Text row) {
        boolean usedScores = false;
        String phraseWithScores = null;
        // loop through the WordsAndScores and if we find one that has scores, generate the "phrase with scores" excerpt
        for (WordsAndScores wordsAndScores : wordsAndScoresArr) {
            if (wordsAndScores == null) {
                continue;
            }
            if (wordsAndScores.getUseScores()) {
                phraseWithScores = generatePhrase(wordsAndScoresArr);
                usedScores = true;
                break;
            }
        }
        // if we did not find any scores in the whole wordsAndScoresArr
        if (!usedScores) {
            phraseWithScores = "XXXNOTSCOREDXXX";
        }
        // if we have any scores, turn off outputting them
        if (usedScores) {
            for (WordsAndScores wordsAndScores : wordsAndScoresArr) {
                if (wordsAndScores == null) {
                    continue;
                }
                if (wordsAndScores.getUseScores()) {
                    wordsAndScores.setOutputScores(false);
                }
            }
        }
        // generate the "phrase without scores" excerpt
        String phraseWithoutScores = generatePhrase(wordsAndScoresArr);
        String oneBestExcerpt;
        // if the regular excerpt is blank, we will return blank excerpts for the other ones also
        if (phraseWithoutScores.isEmpty()) {
            phraseWithScores = "";
            oneBestExcerpt = "";
        } else {
            // if not scored, we won't output anything for this part
            if (!usedScores && startOffset < endOffset) {
                oneBestExcerpt = "XXXNOTSCOREDXXX";
            } else {
                // prepare all the WordsAndScores to output for the "one best" excerpt
                for (WordsAndScores wordsAndScores : wordsAndScoresArr) {
                    if (wordsAndScores == null) {
                        continue;
                    }
                    wordsAndScores.setOneBestExcerpt(true);
                }
                // generate the "one best" excerpt
                oneBestExcerpt = generatePhrase(wordsAndScoresArr);
            }

        }
        // create a key that contains all of our excerpts to be read by the ExcerptTransform
        tk = new Key(row, new Text(dtUid),
                        new Text(fieldName + Constants.NULL + phraseWithScores + Constants.NULL + phraseWithoutScores + Constants.NULL + oneBestExcerpt), cv,
                        ts);
        tv = new Value();
    }

    /**
     * Generate a phrase from the given lists of WordsAndScores
     *
     * @param wordsAndScoresArr
     *            the array of WordsAndScores that contain the terms to create a phrase from
     * @return the phrase
     */
    protected String generatePhrase(WordsAndScores[] wordsAndScoresArr) {
        // put brackets around whole hit phrases instead of individual terms
        checkForHitPhrase(wordsAndScoresArr);
        // If we have no scores, for each WordsAndScores, if we have a hit term that isn't the longest word in its object, set an override so that we output
        // the longest word in that object with brackets instead of the original hit term.
        overrideOutputLongest(wordsAndScoresArr);
        // create an array with the same length as the one we just passed in
        String[] termsToOutput = new String[wordsAndScoresArr.length];
        boolean bef = direction.equals(BEFORE);
        boolean aft = direction.equals(AFTER);
        boolean lock = false;
        int beforeIndex = -1;
        int afterIndex = -1;
        int debugCounter = 0; // FOR DEBUG: counter used for debug logging
        String outputWord; // reusing in loop instead of making a bunch of new strings
        // go through the whole WordsAndScoresArr and try to get a word to output for each offset
        for (int i = 0; i < wordsAndScoresArr.length; i++) {
            // if there is nothing at this position, put nothing at the position in the output
            if (wordsAndScoresArr[i] == null) {
                termsToOutput[i] = null;
            } else {
                // have the WordsAndScores for this position return something to output
                outputWord = wordsAndScoresArr[i].getWordToOutput();
                // If the WordsAndScores returns null, that means the word chosen for this position is something we do not want to output.
                // Put null for this offset in the output.
                if (outputWord == null) {
                    termsToOutput[i] = null;
                    // FOR DEBUG: counting how many things we don't want to output have been removed from the excerpt
                    if (log.isDebugEnabled() && trim) {
                        debugCounter++;
                    }
                } else {
                    // if the term to output is valid, put it in the same position in the new array
                    termsToOutput[i] = outputWord;
                    // if the user has requested BEFORE or AFTER and this object had a hit term...
                    if ((bef || aft) && (wordsAndScoresArr[i].getHasHitTerm())) {
                        // if this is the first hit term for the excerpt, set this offset as the "afterIndex" and set the lock so we do not write over it
                        if (!lock) {
                            afterIndex = i;
                            lock = true;
                        }
                        // set this offset as the "beforeIndex" (no lock on this one because we want it to keep being overwritten with the last hit term offset)
                        beforeIndex = i;
                    }
                }
            }
        }
        // FOR DEBUG:
        if (log.isDebugEnabled() && trim) {
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
        // if no BEFORE or AFTER AND a hit term wasn't found...
        if (!lock) {
            if (!trim) {
                // join everything together with spaces while skipping null offsets
                return joiner.join(termsToOutput);
            } else {
                // join everything together with spaces while skipping null offsets (after trimming both sides down to what we need)
                return joiner.join(bothTrim(termsToOutput));
            }
        } else {
            if (bef) { // if direction is "before", set everything after the last hit term to null
                for (int k = beforeIndex + 1; k < wordsAndScoresArr.length; k++) {
                    termsToOutput[k] = null;
                }
                // trim the excerpt down if we need
                if (trim) {
                    int start = (int) (beforeIndex - (origHalfSize * 2));
                    trimBeginning(termsToOutput, beforeIndex, start);
                }
            } else { // if direction is "after", set everything before the first hit term to null
                for (int k = 0; k < afterIndex; k++) {
                    termsToOutput[k] = null;
                }
                // trim the excerpt down if we need
                if (trim) {
                    int start = (int) (afterIndex + (origHalfSize * 2));
                    trimEnd(termsToOutput, afterIndex, start);
                }
            }
            // join everything together with spaces while skipping null offsets
            return joiner.join(termsToOutput);
        }
    }

    /**
     * Trim down both side of the excerpt to the size that we want
     *
     * @param termsToOutput
     *            the terms to create a phrase from
     * @return the trimmed array
     */
    private String[] bothTrim(String[] termsToOutput) {
        // calculate the midpoint of the expanded start and end offsets (because this should only be triggered on a second attempt)
        int expandedMid = (endOffset - startOffset) / 2;
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
    private void trimBeginning(String[] termsToOutput, int beforeIndex, int start) {
        boolean startNull = false;
        // start at "beforeIndex" and work our way backwards through the array
        for (; beforeIndex >= 0; beforeIndex--) {
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
    private void trimEnd(String[] termsToOutput, int afterIndex, int start) {
        boolean startNull = false;
        // start at "afterIndex" and work our way through the array
        for (; afterIndex < termsToOutput.length; afterIndex++) {
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
        }
    }

    /**
     * Looks for hit phrases (not separate hit terms) and puts the whole phrase in brackets
     *
     * @param wordsAndScoresArr
     *            the terms to create a phrase from
     */
    private void checkForHitPhrase(WordsAndScores[] wordsAndScoresArr) {
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
                    int overridePosition;
                    for (int k = 0; k < individualHitTerms.length; k++) {
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
    private boolean isPhraseFound(String[] individualHitTerms, WordsAndScores[] terms, int j) {
        ArrayList<String> tempWords;
        // k represents what position we are in of the individual hit terms array
        for (int k = 0; k < individualHitTerms.length; k++) {
            // if a WordsAndScores is null, the phrase obviously wasn't found
            if (terms[j + k] == null) {
                return false;
            }
            // get the words list from the current WordsAndScores
            tempWords = (ArrayList<String>) terms[j + k].getWordsList();
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
     * then it will set an override so that the longest word is output with brackets.
     *
     * @param wordsAndScoresArr
     *            the terms to create a phrase from
     */
    private void overrideOutputLongest(WordsAndScores[] wordsAndScoresArr) {
        // check to see if we find scores and if so, return unchanged
        for (WordsAndScores ws : wordsAndScoresArr) {
            if (ws == null) {
                continue;
            }
            if (ws.getUseScores()) {
                return;
            }
        }
        // we now know that scores are not being used for this excerpt
        for (WordsAndScores ws : wordsAndScoresArr) {
            if (ws == null) {
                continue;
            }
            // if a WordsAndScores has a hit term in it...
            if (ws.getHasHitTerm()) {
                // get the index of its longest word
                int lwi = ws.getLongestWordIndex();
                // check to see if the index of its longest word is not the same as the index of the hit term (meaning that the hit term is NOT the longest
                // word)
                if (ws.getHitTermIndex() != lwi) {
                    // get the override value from the WordsAndScores
                    int ov = ws.getOverrideValue();
                    // if this WordsAndScores has a valid override, use the same value but set the index to the index of the longest word
                    if (ov >= 0) {
                        ws.setOverride(lwi, ov);
                    } else {
                        // if it does not have a valid override yet, set it with the index of the longest word and an override value of "4"
                        ws.setOverride(lwi, 4);
                    }
                }
            }
        }
    }

    /**
     * Determine if this dt and uid are in the accepted column families
     *
     * @param dtAndUid
     *            the dt and uid string
     * @return true if we can use it, false if not
     */
    private boolean isUsableDocument(String dtAndUid) {
        return columnFamilies.contains(dtAndUid) == inclusive;
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
    private SortedSet<String> getSortedCFs(Collection<ByteSequence> columnFamilies) {
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
    private String[] getFieldAndValue(Key tfKey) {
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
    private String getDtUidFromTfKey(Key tfKey) {
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
    private String getDtUidFromEventKey(Key eventKey, boolean startKey, boolean inclusive) {
        // if an infinite end range, or unspecified end document, then no document to specify
        if (eventKey == null || eventKey.getColumnFamily() == null || eventKey.getColumnFamily().getLength() == 0) {
            return null;
        }

        // get the dt/uid from the cf
        String cf = eventKey.getColumnFamily().toString();
        String dtAndUid = getDtUid(cf);

        // if calculating a start cq
        if (startKey) {
            // if the start dt/uid is inclusive and the cf is only the dt and uid, then include this document
            if (inclusive && cf.equals(dtAndUid)) {
                return dtAndUid + Constants.NULL;
            }
            // otherwise start at the next document
            else {
                return dtAndUid + Constants.ONE_BYTE;
            }
        }
        // if calculating an end cq
        else {
            // if the end dt/uid is inclusive or the cf was not only the dt and uid
            if (inclusive || !cf.equals(dtAndUid)) {
                // then include this document
                return dtAndUid + Constants.NULL + Constants.MAX_UNICODE_STRING;
            }
            // otherwise stop before this document
            else {
                return dtAndUid + Constants.NULL;
            }
        }
    }

    // get the dt/uid from the beginning of a given string
    private String getDtUid(String str) {
        int index = str.indexOf(Constants.NULL);
        index = str.indexOf(Constants.NULL, index + 1);
        if (index == -1) {
            return str;
        } else {
            return str.substring(0, index);
        }
    }

    public void setHitTermsList(List<String> hitTermsList) {
        this.hitTermsList = (ArrayList<String>) hitTermsList;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public void setOrigHalfSize(float origHalfSize) {
        this.origHalfSize = origHalfSize;
    }

    public void setTrim(boolean trim) {
        this.trim = trim;
    }

    @Override
    public String toString() {
        return "TermFrequencyExcerptIterator: " + this.fieldName + ", " + this.startOffset + ", " + this.endOffset;
    }

}
