package datawave.query.iterator.logic;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.protobuf.TermWeightPosition;

/**
 * An object used to save terms and their respective scores (if a score exists and is valid).
 */
public class WordsAndScores {
    private static final Logger log = LoggerFactory.getLogger(WordsAndScores.class);

    /** the arraylist of words */
    private final ArrayList<String> words;
    /** the arraylist of scores */
    private final ArrayList<Integer> scores;

    /** the index of the word with the longest length */
    private int longestWordIndex;
    /** the index of the word with the smallest score (smallest non-negative is best) */
    private int smallestScoreIndex;
    /** the index of the hit term to output */
    private int hitTermIndex;
    /** the size of the words and scores arrays */
    private int arrSize;

    /** the index of the override to output */
    private int overrideIndex;
    /** 1 for beginning of phrase, 2 for middle of phrase, 3 for end of phrase, anything else for none of those. */
    private int overrideValue;

    private boolean useScores;
    private boolean hasHitTerm;
    private boolean outputScores;
    private boolean oneBestExcerpt;

    private static final String OPENP = "(";
    private static final String CLOSEP = ")";
    private static final String OPENB = "[";
    private static final String CLOSEB = "]";

    /**
     * the list of stop words (words to skip when outputting the excerpt).
     * <p>
     * </p>
     * used by TermFrequencyExcerptIterator.generatePhrase
     */
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
     * A method to add a word and score into the object and saves its index if it is the longest word or has the smallest (non-negative) score.
     *
     * @param word
     *            the word to add
     * @param score
     *            the score to add
     * @return True if the added term is a stop word and false otherwise.
     */
    public boolean addTerm(String word, int score, List<String> hitTermsList) {
        if (score < 0) {
            return addTerm(word, hitTermsList);
        } else {
            arrSize++; // increment the size of the arrays
            words.add(word); // add the word to its array
            scores.add(score); // add the score to its array
            if (arrSize > 1) { // if this is at least the second word/score we are adding...
                if ((hitTermsList != null) && hitTermsList.contains(word)) { // if this word is a hit term...
                    if (hitTermIndex == -1) { // if we don't have a hit term at this offset yet...
                        hitTermIndex = arrSize - 1; // set this index as the one with a hit term
                        hasHitTerm = true;
                    } else {
                        // if we have more than one hit term at this offset, pick the one with the highest score or if no scores, the longest.
                        if ((score < scores.get(hitTermIndex)) || (score == scores.get(hitTermIndex) && (word.length() > words.get(hitTermIndex).length()))) {
                            hitTermIndex = arrSize - 1;
                        }
                    }
                }
                if (word.length() > words.get(longestWordIndex).length()) { // if this word is longer than the current longest word
                    longestWordIndex = arrSize - 1; // set this index as the longest word
                }
                if (smallestScoreIndex != -1) { // if we have a current smallestScore...
                    if (score <= scores.get(smallestScoreIndex)) { // if this score is non-negative and smaller than or equal to the current smallest score...
                        if (score != scores.get(smallestScoreIndex)) { // if this score is smaller than the current smallest score...
                            smallestScoreIndex = arrSize - 1; // set this index as the smallest score
                        } else { // if this score is equal to the smallest score (just in case this happens for some
                            // reason)...
                            log.info("Two tokens have the same score: Choosing the longest one.");
                            if (word.length() > words.get(smallestScoreIndex).length()) { // if this word is longer than the word with the smallest score...
                                smallestScoreIndex = arrSize - 1; // set this index as the smallest score
                            }
                        }
                    }
                } else { // if we do not already have a valid smallestScore...
                    smallestScoreIndex = arrSize - 1; // set this index as the smallest score
                    useScores = true;
                    outputScores = true;
                }
            } else { // if this is the first word/score we are adding, set index 0 (the only one) as the longest/smallest
                longestWordIndex = 0;
                smallestScoreIndex = 0;
                useScores = true;
                outputScores = true;
                if ((hitTermsList != null) && hitTermsList.contains(word)) {
                    hitTermIndex = 0;
                    hasHitTerm = true;
                }
            }
            return STOP_WORD_LIST.contains(word);
        }
    }

    /**
     * A method to add a word into the object and saves its index if it is the longest word.
     *
     * @param word
     *            the word to add
     * @return True if the added term is a stop word and false otherwise.
     */
    public boolean addTerm(String word, List<String> hitTermsList) {
        arrSize++; // increment array size
        words.add(word); // add the word to its array
        scores.add(-1); // add the default score to its array
        if (arrSize > 1) { // if this is at least the second word we are adding...
            if ((hitTermsList != null) && hitTermsList.contains(word)) { // if this word is a hit term...
                if (hitTermIndex == -1) { // if we don't have a hit term at this offset yet...
                    hitTermIndex = arrSize - 1; // set this index as the one with a hit term
                    hasHitTerm = true;
                } else {
                    if ((word.length() > words.get(hitTermIndex).length())) { // if we have another hit term at this offset, pick the longest one.
                        hitTermIndex = arrSize - 1;
                    }
                }
            }
            if (word.length() > words.get(longestWordIndex).length()) { // if this word is longer than the current longest word...
                longestWordIndex = arrSize - 1; // set this index as the longest word
            }
        } else { // if this is the first word we are adding set the longest word index to 0 (the only one)
            longestWordIndex = 0;
            if ((hitTermsList != null) && hitTermsList.contains(word)) {
                hitTermIndex = 0;
                hasHitTerm = true;
            }
        }
        return STOP_WORD_LIST.contains(word);
    }

    /**
     * A method to return whichever word we are going to use in the excerpt for this position.
     *
     * @return the word with the greatest score if the score is valid, if not, return the longest word
     */
    public String getWordToOutput() {
        if (smallestScoreIndex == -1 && longestWordIndex == -1) { // if we try and get the word from an object with nothing added to it (should never happen)...
            log.warn("Trying to get token to output when none have been added: Will output \"REPORTMETODATAWAVE\".");
            return "REPORTMETODATAWAVE";
        } else {
            if (oneBestExcerpt) {
                if (scores.get(smallestScoreIndex) > 90000000) {
                    return null;
                }
                if (STOP_WORD_LIST.contains(words.get(smallestScoreIndex))) { // if the selected term is in the stop list...
                    return null;
                }
                if (hitTermIndex == smallestScoreIndex) {
                    return OPENB + words.get(hitTermIndex) + CLOSEB;
                } else {
                    return words.get(smallestScoreIndex);
                }
            }
            if (overrideIndex >= 0 && overrideIndex < arrSize) {
                if (scores.get(overrideIndex) > 90000000) {
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
            if (hasHitTerm) { // if we have a hit term...
                if (scores.get(hitTermIndex) > 90000000) {
                    return null;
                }
                if (STOP_WORD_LIST.contains(words.get(hitTermIndex))) { // if the hit term is on the stop list for some reason...
                    return null;
                }
                if (useScores && (scores.get(hitTermIndex) != -1)) { // if we have a valid score for the hit term...
                    return OPENB + words.get(hitTermIndex) + OPENP + userReadable(scores.get(hitTermIndex)) + CLOSEP + CLOSEB;
                } else {
                    return OPENB + words.get(hitTermIndex) + CLOSEB;
                }
            }
            if (useScores) { // if we have added at least one score, and it is a valid score...
                if (scores.get(smallestScoreIndex) > 90000000) {
                    return null;
                }
                if (STOP_WORD_LIST.contains(words.get(smallestScoreIndex))) { // if the selected term is in the stop list...
                    return null;
                }
                if (outputScores) {
                    return words.get(smallestScoreIndex) + OPENP + userReadable(scores.get(smallestScoreIndex)) + CLOSEP; // return the word with the smallest
                                                                                                                          // score
                } else {
                    return words.get(smallestScoreIndex);
                }
            } else { // default to returning the longest word if the scores don't exist/aren't valid
                if (STOP_WORD_LIST.contains(words.get(longestWordIndex))) { // if the selected term is in the stop list...
                    return null;
                }
                return words.get(longestWordIndex); // return the longest word
            }
        }
    }

    /**
     * Converts the score into a number from 0-100 (higher is better) so that it is easier for the user to understand.
     */
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
