package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;

/**
 * A simple No-Op implementation of a {@link LanguageAwareAnalyzer}
 */
public class NoOpLuceneAnalyzer extends LanguageAwareAnalyzer {

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        return stream;
    }

    @Override
    protected TokenStream getStopWordFilter(TokenStream stream) {
        return stream;
    }

    @Override
    protected TokenStream getStemExclusions(TokenStream stream) {
        return stream;
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return stream;
    }

    @Override
    protected TokenStream getLemmatization(TokenStream stream) {
        // method body is same as super method on purpose, explicit no-op
        return stream;
    }

    @Override
    public CharArraySet getStopwordSet() {
        return CharArraySet.EMPTY_SET;
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return CharArraySet.EMPTY_SET;
    }
}
