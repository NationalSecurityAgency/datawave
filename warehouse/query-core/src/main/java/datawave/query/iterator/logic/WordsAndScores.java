package datawave.query.iterator.logic;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * An object used to save terms and their respective scores (if a score exists and is valid).
 */
public class WordsAndScores {
    private static final Logger log = Logger.getLogger(WordsAndScores.class);

    /** the arraylist of words */
    private ArrayList<String> words;
    /** the arraylist of scores */
    private ArrayList<Integer> scores;

    /** the index of the word with the longest length */
    private int longestWordIndex;
    /** the index of the word with the smallest score (smallest non-negative is best) */
    private int smallestScoreIndex;
    /** the size of the words and scores arrays */
    private int arrSize;

    private boolean useScores;

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
        arrSize = 0;
        useScores = false;
    }

    public void reset() {
        words.clear();
        scores.clear();
        longestWordIndex = -1;
        smallestScoreIndex = -1;
        arrSize = 0;
        useScores = false;
    }

    /**
     * A method to add a word and score into the object and saves its index if it is the longest word or has the smallest (non-negative) score.
     *
     * @param word
     *            the word to add
     * @param score
     *            the score to add
     */
    public void addTerm(String word, int score) {
        arrSize++; // increment the size of the arrays
        words.add(word); // add the word to its array
        scores.add(score); // add the score to its array
        if (arrSize > 1) { // if this is at least the second word/score we are adding...
            if (word.length() > words.get(longestWordIndex).length()) { // if this word is longer than the current longest word
                longestWordIndex = arrSize - 1; // set this index as the longest word
            }
            if (smallestScoreIndex == -1) { // if we do not already have a valid smallestScore...
                if (score >= 0) { // if the passed in score is valid...
                    smallestScoreIndex = arrSize - 1; // set this index as the smallest score
                    useScores = true;
                }
            } else { // if we have a current smallestScore...
                if (score >= 0 && score <= scores.get(smallestScoreIndex)) { // if this score is non-negative and smaller than or equal to the current smallest
                                                                             // score...
                    if (score == scores.get(smallestScoreIndex)) { // if this score is equal to the smallest score (just in case this happens for some
                                                                   // reason)...
                        log.info("Two tokens have the same score: Choosing the longest one.");
                        if (word.length() > words.get(smallestScoreIndex).length()) { // if this word is longer than the word with the smallest score...
                            smallestScoreIndex = arrSize - 1; // set this index as the smallest score
                        }
                    } else { // if this score is smaller than the current smallest score...
                        smallestScoreIndex = arrSize - 1; // set this index as the smallest score
                    }
                }
            }
        } else { // if this is the first word/score we are adding, set index 0 (the only one) as the longest/smallest
            longestWordIndex = 0;
            if (score >= 0) {
                smallestScoreIndex = 0;
                useScores = true;
            }
        }
    }

    /**
     * A method to add a word into the object and saves its index if it is the longest word.
     *
     * @param word
     *            the word to add
     */
    public void addTerm(String word) {
        arrSize++; // increment array size
        words.add(word); // add the word to its array
        scores.add(-1); // add the default score to its array
        if (arrSize > 1) { // if this is atleast the second word we are adding...
            if (word.length() > words.get(longestWordIndex).length()) { // if this word is longer than the current longest word...
                longestWordIndex = arrSize - 1; // set this index as the longest word
            }
        } else { // if this is the first word we are adding set the longest word index to 0 (the only one)
            longestWordIndex = 0;
        }
    }

    /**
     * A method to return whichever word we are going to use in the excerpt for this position.
     *
     * @return the word with the greatest score if the score is valid, if not, return the longest word
     */
    public String getWordToOutput() {
        if (smallestScoreIndex == -1 && longestWordIndex == -1) { // if we try and get the word from an object with nothing added to it (should never happen)...
            log.warn("Trying to get token to output when none have been added: Will output \"reportmetodatawave\".");
            return "reportmetodatawave";
        } else {
            if (useScores) { // if we have added at least one score, and it is a valid score...
                return words.get(smallestScoreIndex); // return the word with the smallest score
            } else { // default to returning the longest word if the scores don't exist/aren't valid
                return words.get(longestWordIndex); // return the longest word
            }
        }
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
    public void setWordsList(List<String> words) {
        reset();
        for (String word : words) {
            addTerm(word);
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
     * Sets the list of scores to the one passed in.
     *
     * @param scores
     *            a list of scores
     */
    public void setScoresList(List<Integer> scores) {
        this.scores = (ArrayList<Integer>) scores;
    }

    /**
     * Sets the list of words and the list of scores to the ones passed in.
     *
     * @param words
     *            a list of words
     * @param scores
     *            a list of scores
     */
    public void setWordsAndScoresList(List<String> words, List<Integer> scores) {
        if (words.size() != scores.size()) {
            throw new IllegalArgumentException("The words and scores lists must be the same size!");
        } else {
            reset();
            for (int i = 0; i < words.size(); i++) {
                addTerm(words.get(i), scores.get(i));
            }
        }
    }

    /**
     * Returns a boolean that is true if there is a valid score in the scores list and false otherwise.
     *
     * @return useScores
     */
    public boolean getUseScores() {
        return useScores;
    }

    public int getArrSize() {
        return arrSize;
    }

    @Override
    public String toString() {
        return "WordsAndScores{" + "words=" + words + ", scores=" + scores + ", longestWordIndex=" + longestWordIndex + ", smallestScoreIndex="
                        + smallestScoreIndex + ", arrSize=" + arrSize + '}';
    }
}
