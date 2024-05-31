package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.de.GermanLightStemFilter;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the German language
 */
public class GermanLuceneAnalyzer extends LanguageAwareAnalyzer {

    public GermanLuceneAnalyzer() {
        super(GermanAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        stream = new GermanNormalizationFilter(stream);
        return new GermanLightStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return GermanAnalyzer.getDefaultStopSet();
    }
}
