package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.RussianStemmer;

/**
 * An example {@link LanguageAwareAnalyzer} for the Russian language
 */
public class RussianLuceneAnalyzer extends LanguageAwareAnalyzer {

    public RussianLuceneAnalyzer() {
        super(RussianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    protected SnowballProgram getStemmer() {
        return new RussianStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return RussianAnalyzer.getDefaultStopSet();
    }
}
