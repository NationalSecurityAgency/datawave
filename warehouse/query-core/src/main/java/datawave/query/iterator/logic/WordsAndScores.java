package datawave.query.iterator.logic;

import java.util.ArrayList;
import java.util.Set;

public class WordsAndScores {
    private final ArrayList<String> words;
    private final ArrayList<Integer> scores;

    private int longestWordIndex;
    private int biggestScoreIndex;
    private int arrSize;

    public static final Set<String> STOP_WORD_LIST = Set.of("<eps>");

    public WordsAndScores() {
        words = new ArrayList<>();
        scores = new ArrayList<>();
        longestWordIndex = -1;
        biggestScoreIndex = -1;
        arrSize = 0;
    }

    public void addTerm(String word, int score) {
        arrSize++;
        words.add(word);
        scores.add(score);
        if (arrSize > 1) {
            if (word.length() > words.get(arrSize - 2).length()) {
                longestWordIndex = arrSize - 1;
            }
            if (score <= scores.get(arrSize - 2) && score >= 0) {
                if (score < scores.get((arrSize - 2))) {
                    biggestScoreIndex = arrSize - 1;
                } else {
                    if (word.length() > words.get(biggestScoreIndex).length()) {
                        biggestScoreIndex = arrSize - 1;
                    }
                }
            }
        } else {
            longestWordIndex = 0;
            biggestScoreIndex = 0;
        }
    }

    public void addTerm(String word) {
        arrSize++;
        words.add(word);
        scores.add(-1);
        if (arrSize > 1) {
            if (word.length() > words.get(arrSize - 2).length()) {
                longestWordIndex = arrSize - 1;
            }
        } else {
            longestWordIndex = 0;
        }
    }

    public String getWordToOutput() {
        if (biggestScoreIndex != -1 && scores.get(biggestScoreIndex) != -1) {
            return words.get(biggestScoreIndex);
        } else {
            return words.get(longestWordIndex);
        }
    }
}
