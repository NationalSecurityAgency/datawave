package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fa.PersianNormalizationFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Persian language
 */
public class PersianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected PersianLuceneAnalyzer() {
        super(PersianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    /**
     * Order of operations is different in the PersianAnalyzer, so preserve that here
     *
     * @param fieldName
     *            the field name
     * @return the token stream
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new StandardTokenizer();
        TokenStream result = new LowerCaseFilter(source);
        result = new DecimalDigitFilter(result);
        result = new ArabicNormalizationFilter(result);
        /* additional persian-specific normalization */
        result = new PersianNormalizationFilter(result);
        /*
         * the order here is important: the stopword list is normalized with the above!
         */
        if (stopWordsEnabled) {
            result = new StopFilter(result, getStopWords());
        }

        return new TokenStreamComponents(source, result);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return PersianAnalyzer.getDefaultStopSet();
    }
}
