package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.RomanianStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Romanian language
 */
public class RomanianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected RomanianLuceneAnalyzer() {
        super(RomanianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected SnowballProgram getStemmer() {
        return new RomanianStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return RomanianAnalyzer.getDefaultStopSet();
    }
}
