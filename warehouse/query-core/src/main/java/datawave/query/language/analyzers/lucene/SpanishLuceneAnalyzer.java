package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.es.SpanishLightStemFilter;

/**
 * An example {@link LanguageAwareAnalyzer} for the Spanish language
 */
public class SpanishLuceneAnalyzer extends LanguageAwareAnalyzer {

    public SpanishLuceneAnalyzer() {
        super(SpanishAnalyzer.getDefaultStopSet());
    }

    protected TokenStream getStemFilter(TokenStream stream) {
        return new SpanishLightStemFilter(stream);
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return SpanishAnalyzer.getDefaultStopSet();
    }
}
