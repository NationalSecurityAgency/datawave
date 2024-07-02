package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianStemFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the Bulgarian language
 */
public class BulgarianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected BulgarianLuceneAnalyzer() {
        super(BulgarianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    protected TokenStream getStemFilter(TokenStream stream) {
        return new BulgarianStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return BulgarianAnalyzer.getDefaultStopSet();
    }
}
