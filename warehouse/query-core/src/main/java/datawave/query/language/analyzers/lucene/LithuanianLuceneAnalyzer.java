package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.LithuanianStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Lithuanian language
 */
public class LithuanianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected LithuanianLuceneAnalyzer() {
        super(LithuanianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected SnowballProgram getStemmer() {
        return new LithuanianStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return LithuanianAnalyzer.getDefaultStopSet();
    }
}
