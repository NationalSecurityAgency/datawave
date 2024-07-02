package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianStemFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the Indonesian language
 */
public class IndonesianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected IndonesianLuceneAnalyzer() {
        super(IndonesianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new IndonesianStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return IndonesianAnalyzer.getDefaultStopSet();
    }
}
