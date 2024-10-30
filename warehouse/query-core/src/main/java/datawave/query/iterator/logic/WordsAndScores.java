package datawave.query.iterator.logic;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.PositiveOrZero;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.protobuf.TermWeightPosition;

/**
 * An object used to save terms and their respective scores (if a score exists and is valid). <br>
 * <br>
 * Each instance represents the word/words at a single position(offset) in a document, and the <code>addWord</code> method is used to record the words found in
 * that position. <br>
 * There are conditions where multiple words may exist in the same position in a document, such as synonym tokens for standard text or speech-to-text where
 * multiple possible readings for a single utterance exist. <br>
 * Individual words may be stop words that we do not want to emit as a component of an excerpt, but we need to track these, because in the speech to text case
 * there may be poorer choices (e.g., with lower scores) that we want to avoid emitting. <br>
 * <br>
 * In addition to preserving state, this class implements the <code>getWordToOutput</code> method that chooses the best word from this position to emit as a
 * part of a hit excerpt, employing a variety of rules including everything from score to word length or the word's position in a phrase.
 */
public class WordsAndScores {
    private static final Logger log = LoggerFactory.getLogger(WordsAndScores.class);

    public static final int MAX_ARRAY_SIZE = 1024;
    public static final int MAX_SCORE = 900_000_000;

    /** the list of words, there may be one or many words saved for a position in the document */
    private final ArrayList<String> words;
    /**
     * the list of scores, each word for a single position may have a different score. <br>
     * the indexes in this list correspond to the indexes in the <code>words</code> array
     */
    private final ArrayList<Integer> scores;

    /** the index of the word with the longest length */
    private int longestWordIndex;
    /** the index of the word with the smallest score (smallest non-negative is best) */
    private int smallestScoreIndex;
    /** the index of the hit term to output */
    private int hitTermIndex;
    /** the size of the words and scores lists */
    @PositiveOrZero
    private int arrSize;

    /**
     * the index of the override to output - Except for when creating the "one-best" excerpt, the override has the highest priority for being output. <br>
     * It should be used when we want to output something different from the standard decision-making in <code>getWordToOutput()</code>. <br>
     * External logic can use <code>setOverride(index, value)</code> to set.
     */
    private int overrideIndex;
    /**
     * 1 for beginning of phrase, 2 for middle of phrase, 3 for end of phrase, 4 for brackets around the specified word, anything else for none of those. <br>
     * <br>
     * It should be used when we want to output something different from the standard decision-making in <code>getWordToOutput()</code>. <br>
     * External logic can use <code>setOverride(index, value)</code> to set.
     */
    private int overrideValue;

    private boolean useScores;
    private boolean hasHitTerm;
    private boolean outputScores;
    private boolean oneBestExcerpt;

    private static final String OPENP = "(";
    private static final String CLOSEP = ")";
    private static final String OPENB = "[";
    private static final String CLOSEB = "]";

    /** the list of stop words (words to skip when outputting the excerpt). */
    public static final Set<String> STOP_WORD_LIST = Set.of("<eps>");

    public WordsAndScores() {
        words = new ArrayList<>();
        scores = new ArrayList<>();
        longestWordIndex = -1;
        smallestScoreIndex = -1;
        hitTermIndex = -1;
        arrSize = 0;
        useScores = false;
        hasHitTerm = false;
        overrideIndex = -1;
        overrideValue = -1;
        outputScores = false;
        oneBestExcerpt = false;
    }

    /**
     * A method to add a word the object and update the internal state that tracks the best longest term and the best hit term. The added term may be a stop
     * word, meaning that we don't want to output it as an excerpt. In certain cases, the stop word might be better than other choices, so we need to track them
     * all.
     *
     * @param word
     *            the word to add
     * @return True if the added term is a stop word and false otherwise.
     */
    public boolean addTerm(String word, List<String> hitTermsList) {
        return addTerm(word, -1, hitTermsList);
    }

    /**
     * A method to add a word and score into the object and update the internal state that tracks the best longest term, the best score and the best hit term.
     * The added term may be a stop word, meaning that we don't want to output it as an excerpt. In certain cases, the stop word might be better than other
     * choices, so we need to track them all.
     *
     * @param word
     *            the word to add
     * @param score
     *            the score to add
     * @return True if the added term is a stop word and false otherwise.
     */
    public boolean addTerm(String word, int score, List<String> hitTermsList) {
        final int currentIndex = arrSize;
        arrSize++;

        if (arrSize > MAX_ARRAY_SIZE) {
            log.info("WordsAndScores encountered more than the maximum number of words in this position, ignoring word: {} with score: {}", word, score);
            return false;
        }

        words.add(word);
        // if a score is less than 0, replace it with -1;
        scores.add(score >= 0 ? score : -1);

        updateLongestWordIndex(word, currentIndex);
        // we only want to update with non-negative scores
        if (score >= 0) {
            updateBestScoreIndex(word, score, currentIndex);
        }

        if ((hitTermsList != null) && hitTermsList.contains(word)) {
            updateHitTermIndex(word, score, currentIndex);
        }

        return STOP_WORD_LIST.contains(word);
    }

    /**
     * Update the longest word index if the specified word is longer than the current longest word
     *
     * @param word
     *            the word to check.
     * @param currentIndex
     *            the current index we're inserting into.
     */
    private void updateLongestWordIndex(String word, int currentIndex) {
        if (currentIndex == 0) {
            longestWordIndex = 0;
            return;
        }

        if (word.length() > words.get(longestWordIndex).length()) { // if this word is longer than the current longest word
            longestWordIndex = currentIndex; // set this index as the longest word
        }
    }

    /**
     * Update the best score index if the specified word has a better score than the current best score (the smallest non-negative score is the best score)
     *
     * @param word
     *            the word to check.
     * @param score
     *            the word's score. (we have already )
     * @param currentIndex
     *            the current index we're inserting into.
     */
    private void updateBestScoreIndex(String word, @PositiveOrZero int score, int currentIndex) {
        if (smallestScoreIndex == -1) {
            // if we have no valid smallestScore, choose the current index
            smallestScoreIndex = currentIndex;
            // we've received at least one valid score, flip these flags to true
            useScores = true;
            outputScores = true;
            return;
        }

        final int smallestScore = scores.get(smallestScoreIndex);
        if (score < smallestScore) {
            // if the current score is smaller, choose the current index
            smallestScoreIndex = currentIndex;
            return;
        }

        if (score == smallestScore) {
            // if this score is equal to the smallest score, choose the longer word
            log.info("Two tokens have the same score: Choosing the longest one.");
            if (word.length() > words.get(smallestScoreIndex).length()) {
                smallestScoreIndex = currentIndex;
            }
        }
    }

    /**
     * Update the index of the best hit term index if the specified word is a better hit term than the current hit term.
     *
     * @param word
     *            the word to check.
     * @param score
     *            the word's score.
     * @param currentIndex
     *            the current index we're inserting into.
     */
    private void updateHitTermIndex(String word, int score, int currentIndex) {
        if (hitTermIndex == -1) { // if we don't have a hit term at this offset yet...
            hitTermIndex = currentIndex; // set this index as the one with a hit term
            hasHitTerm = true;
            return;
        }

        final int hitTermScore = scores.get(hitTermIndex);

        // whether this word is longer than the existing best hit term word.
        boolean currentTermIsLonger = word.length() > words.get(hitTermIndex).length();

        // there are a couple cases here
        // - if current has a score, and it's better than the best, we take current.
        // - if current and best have equal scores, but current is longer, we take current instead of best
        if (score >= 0 && hitTermScore > score) {
            // valid current score and worse valid hitTermScore
            hitTermIndex = currentIndex;
        } else if ((hitTermScore == score || (hitTermScore < 0 && score < 0)) && currentTermIsLonger) {
            // equal current score and hit term score, choose the longer word.
            hitTermIndex = currentIndex;
        }
    }

    /**
     * A method to return whichever word we are going to use in the excerpt for this position. In order of preference
     * <ul>
     * <li>one best excerpt, if enabled</li>
     * <li>overridden word, if this has been provided externally</li>
     * <li>the best hit term word, if present</li>
     * <li>the best scored word</li>
     * <li>the longest word</li>
     * </ul>
     * Each of these will be checked to see if they're stopwords. IF they are, returns null instead.
     *
     * @return the chosen word or null if it is something we do not want to output
     */
    public String getWordToOutput() {
        if (smallestScoreIndex == -1 && longestWordIndex == -1) { // if we try and get the word from an object with nothing added to it (should never happen)...
            log.warn("Trying to get token to output when none have been added: Will output \"REPORTMETODATAWAVE\".");
            return "REPORTMETODATAWAVE";
        }

        if (oneBestExcerpt) {
            return getOneBestWordToOutput();
        }

        if (overrideIndex >= 0 && overrideIndex < arrSize) {
            return getOverrideWordToOutput();
        }

        if (hasHitTerm) { // if we have a hit term...
            return getHitTermWordToOutput();
        }

        if (useScores) { // if we have added at least one score, and it is a valid score...
            return getScoredWordToOutput();
        }

        // default to returning the longest word if the scores don't exist/aren't valid
        if (STOP_WORD_LIST.contains(words.get(longestWordIndex))) { // if the selected term is in the stop list...
            return null;
        }
        return words.get(longestWordIndex); // return the longest word
    }

    /**
     * A method to return the speech-to-text 'one-best' word to output.
     *
     * @return the one-best word, or null if the word is a stopword
     */
    private String getOneBestWordToOutput() {
        if (scores.get(smallestScoreIndex) > MAX_SCORE) {
            return null;
        }
        if (STOP_WORD_LIST.contains(words.get(smallestScoreIndex))) { // if the selected term is in the stop list...
            return null;
        }
        return hitTermIndex == smallestScoreIndex ? OPENB + words.get(hitTermIndex) + CLOSEB : words.get(smallestScoreIndex);
    }

    /**
     * A method to return the overridden word to output, which is identified externally to this code.
     *
     * @return the overridden word, or null if the word is a stopword or exceeds MAX_SCORE.
     */
    private String getOverrideWordToOutput() {
        if (scores.get(overrideIndex) > MAX_SCORE) {
            return null;
        }
        if (STOP_WORD_LIST.contains(words.get(overrideIndex))) { // if the hit term is on the stop list for some reason...
            return null;
        }
        switch (overrideValue) {
            case 1:
                if (useScores && (scores.get(overrideIndex) != -1)) {
                    return OPENB + words.get(overrideIndex) + OPENP + userReadable(scores.get(overrideIndex)) + CLOSEP;
                } else {
                    return OPENB + words.get(overrideIndex);
                }
            case 3:
                if (useScores && (scores.get(overrideIndex) != -1)) {
                    return words.get(overrideIndex) + OPENP + userReadable(scores.get(overrideIndex)) + CLOSEP + CLOSEB;
                } else {
                    return words.get(overrideIndex) + CLOSEB;
                }
            case 2:
                if (useScores && (scores.get(overrideIndex) != -1)) {
                    return words.get(overrideIndex) + OPENP + userReadable(scores.get(overrideIndex)) + CLOSEP;
                } else {
                    return words.get(overrideIndex);
                }
            case 4:
                return OPENB + words.get(overrideIndex) + CLOSEB;
            default:
                log.warn("Invalid override value {}: Will output \"REPORTMETODATAWAVE\".", overrideValue);
                return "REPORTMETODATAWAVE";
        }
    }

    /**
     * A method to return the hit term to output,
     *
     * @return the hit term, or null if the word is a stopword or exceeds MAX_SCORE.
     */
    private String getHitTermWordToOutput() {
        if (scores.get(hitTermIndex) > MAX_SCORE) {
            return null;
        }

        final String hitTerm = words.get(hitTermIndex);

        if (STOP_WORD_LIST.contains(hitTerm)) { // if the hit term is on the stop list for some reason...
            return null;
        }

        final int hitTermScore = scores.get(hitTermIndex);

        if (useScores && (hitTermScore != -1)) { // if we have a valid score for the hit term...
            return OPENB + hitTerm + OPENP + userReadable(hitTermScore) + CLOSEP + CLOSEB;
        } else {
            return OPENB + hitTerm + CLOSEB;
        }
    }

    /**
     * A method to return the word with the best (lowest) score if present.
     *
     * @return the best scored word or null if the word is a stop word or exceeds the max score.
     */
    private String getScoredWordToOutput() {
        final int bestWordScore = scores.get(smallestScoreIndex);

        if (bestWordScore > MAX_SCORE) {
            return null;
        }

        final String bestWord = words.get(smallestScoreIndex);

        if (STOP_WORD_LIST.contains(bestWord)) { // if the selected term is in the stop list...
            return null;
        }

        return outputScores ? bestWord + OPENP + userReadable(bestWordScore) + CLOSEP : bestWord;
    }

    /** Converts the score into a number from 0-100 (higher is better) so that it is easier for the user to understand. */
    private int userReadable(int score) {
        // the original probability got put through ln(x) so we do e^x to put it back to the original probability
        return (int) Math.round((Math.exp(TermWeightPosition.termWeightScoreToPositionScore(score))) * 100);
    }

    /**
     * Returns the list of words saved in this object.
     *
     * @return words
     */
    public List<String> getWordsList() {
        return words;
    }

    /**
     * Sets the list of words to the one passed in and sets the list of scores to -1.
     *
     * @param words
     *            a list of words
     */
    public void setWordsList(List<String> words, List<String> hitTermsList) {
        reset();
        for (String word : words) {
            addTerm(word, hitTermsList);
        }
    }

    /**
     * Returns the list of scores saved in this object.
     *
     * @return scores
     */
    public List<Integer> getScoresList() {
        return scores;
    }

    /**
     * Sets the list of words and the list of scores to the ones passed in.
     *
     * @param words
     *            a list of words
     * @param scores
     *            a list of scores
     */
    public void setWordsAndScoresList(List<String> words, List<Integer> scores, List<String> hitTermsList) {
        if (words.size() != scores.size()) {
            throw new IllegalArgumentException("The words and scores lists must be the same size!");
        } else {
            reset();
            for (int i = 0; i < words.size(); i++) {
                addTerm(words.get(i), scores.get(i), hitTermsList);
            }
        }
    }

    public void setOverride(int overrideIndex, int overrideValue) {
        this.overrideIndex = overrideIndex;
        this.overrideValue = overrideValue;
    }

    public void setOutputScores(boolean outputScores) {
        this.outputScores = outputScores;
    }

    public void setOneBestExcerpt(boolean oneBestExcerpt) {
        this.oneBestExcerpt = oneBestExcerpt;
    }

    /**
     * Returns a boolean that is true if there is a valid score in the scores list and false otherwise.
     *
     * @return useScores
     */
    public boolean getUseScores() {
        return useScores;
    }

    /**
     * Returns a boolean that is true if there is a hit term in the words list and false otherwise.
     *
     * @return hasHitTerm
     */
    public boolean getHasHitTerm() {
        return hasHitTerm;
    }

    public int getArrSize() {
        return arrSize;
    }

    public int getLongestWordIndex() {
        return longestWordIndex;
    }

    public int getHitTermIndex() {
        return hitTermIndex;
    }

    public int getOverrideIndex() {
        return overrideIndex;
    }

    public int getOverrideValue() {
        return overrideValue;
    }

    public void reset() {
        words.clear();
        scores.clear();
        longestWordIndex = -1;
        smallestScoreIndex = -1;
        hitTermIndex = -1;
        arrSize = 0;
        useScores = false;
        hasHitTerm = false;
        overrideIndex = -1;
        overrideValue = -1;
        outputScores = false;
        oneBestExcerpt = false;
    }

    @Override
    public String toString() {
        return "WordsAndScores{" + "words=" + words + ", scores=" + scores + ", longestWordIndex=" + longestWordIndex + ", smallestScoreIndex="
                        + smallestScoreIndex + ", arrSize=" + arrSize + '}';
    }
}
