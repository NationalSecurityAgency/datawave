package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cjk.CJKBigramFilter;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Example {@link LanguageAwareAnalyzer} for Chinese, Japanese and Korean languages
 */
public class CJKLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected CJKLuceneAnalyzer() {
        super(CJKAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    /**
     * Instead of override individual methods, override the whole createComponents method
     *
     * @param fieldName
     *            the field name
     * @return a token stream
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new StandardTokenizer();
        // run the widthfilter first before bigramming, it sometimes combines characters.
        TokenStream result = new CJKWidthFilter(source);
        result = new LowerCaseFilter(result);
        result = new CJKBigramFilter(result);
        return new TokenStreamComponents(source, new StopFilter(result, getStopWords()));
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return CJKAnalyzer.getDefaultStopSet();
    }
}
