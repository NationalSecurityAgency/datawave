package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.gl.GalicianStemFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the Galician language
 */
public class GalicianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected GalicianLuceneAnalyzer() {
        super(GalicianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new GalicianStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return GalicianAnalyzer.getDefaultStopSet();
    }
}
