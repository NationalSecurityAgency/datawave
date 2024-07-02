package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.cz.CzechStemFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the Czech language
 */
public class CzechLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected CzechLuceneAnalyzer() {
        super(CzechAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new CzechStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return CzechAnalyzer.getDefaultStopSet();
    }
}
