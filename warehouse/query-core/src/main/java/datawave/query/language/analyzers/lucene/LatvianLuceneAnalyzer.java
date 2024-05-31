package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianStemFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the Latvian language
 */
public class LatvianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected LatvianLuceneAnalyzer() {
        super(LatvianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new LatvianStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return LatvianAnalyzer.getDefaultStopSet();
    }
}
