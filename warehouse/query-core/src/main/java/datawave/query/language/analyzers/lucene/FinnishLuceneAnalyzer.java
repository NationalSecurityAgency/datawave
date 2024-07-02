package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.FinnishStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Finnish language
 */
public class FinnishLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected FinnishLuceneAnalyzer() {
        super(FinnishAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected SnowballProgram getStemmer() {
        return new FinnishStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return FinnishAnalyzer.getDefaultStopSet();
    }
}
