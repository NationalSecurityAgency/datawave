package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.el.GreekLowerCaseFilter;
import org.apache.lucene.analysis.el.GreekStemFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the Greek language
 */
public class GreekLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected GreekLuceneAnalyzer() {
        super(GreekAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        return new GreekLowerCaseFilter(stream);
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new GreekStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return GreekAnalyzer.getDefaultStopSet();
    }
}
