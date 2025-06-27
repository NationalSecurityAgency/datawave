package datawave.util.keyword.language;

import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ibm.icu.text.BreakIterator;

public class BaseYakeLanguageTest {
    @Test
    public void testGetSentenceBreakIterators() {
        // ensure we can get sentence break iterators for all supported languages.
        String input = "A man. A plan. A canal. Panama!";
        BaseYakeLanguage[] languages = BaseYakeLanguage.values();
        for (BaseYakeLanguage language : languages) {
            List<String> sentences = new ArrayList<>();
            BreakIterator sentenceBreakIterator = language.getSentenceBreakIterator();
            sentenceBreakIterator.setText(input);
            int start = sentenceBreakIterator.first();
            for (int end = sentenceBreakIterator.next(); end != BreakIterator.DONE; start = end, end = sentenceBreakIterator.next()) {
                String sentence = input.substring(start, end).trim();
                if (!sentence.isBlank()) {
                    sentences.add(sentence);
                }
            }
            assertFalse("No sentences produced for language " + language, sentences.isEmpty());
        }
    }
}
