package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hi.HindiNormalizationFilter;
import org.apache.lucene.analysis.hi.HindiStemFilter;
import org.apache.lucene.analysis.in.IndicNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Hindi language
 */
public class HindiLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected HindiLuceneAnalyzer() {
        super(HindiAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    /**
     * Order of operations changes from the base 'createComponents' method and the HindiAnalyzer's 'createComponents'
     *
     * @param fieldName
     *            the field name
     * @return a token stream
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new StandardTokenizer();
        TokenStream stream = new LowerCaseFilter(source);
        stream = new DecimalDigitFilter(stream);
        if (stemExclusionsEnabled) {
            stream = new SetKeywordMarkerFilter(stream, getStemExclusions());
        }
        stream = new IndicNormalizationFilter(stream);
        stream = new HindiNormalizationFilter(stream);
        if (stopWordsEnabled) {
            stream = new StopFilter(stream, getStopWords());
        }
        if (stemmingEnabled) {
            stream = new HindiStemFilter(stream);
        }
        return new TokenStreamComponents(source, stream);
    }

    @Override
    public CharArraySet getStopwordSet() {
        return super.getStopwordSet();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return HindiAnalyzer.getDefaultStopSet();
    }
}
