package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.th.ThaiTokenizer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Thai language
 */
public class ThaiLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected ThaiLuceneAnalyzer() {
        super(ThaiAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    protected Tokenizer getTokenizer() {
        return new ThaiTokenizer();
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        stream = new LowerCaseFilter(stream);
        return new DecimalDigitFilter(stream);
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
    public CharArraySet getDefaultStopWords() {
        return ThaiAnalyzer.getDefaultStopSet();
    }
}
