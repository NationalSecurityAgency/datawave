package datawave.query.language.analyzers.lucene;

import java.util.List;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.ga.IrishLowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.tartarus.snowball.ext.IrishStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Irish language
 */
public class IrishLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected IrishLuceneAnalyzer() {
        super(IrishAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    /**
     * The order of operations for Irish is different enough that the whole method is overriden
     *
     * @param fieldName
     *            the field name
     * @return a token stream
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        CharArraySet hyphenations = new CharArraySet(List.of("h", "n", "t"), true);
        CharArraySet articles = new CharArraySet(List.of("d", "m", "b"), true);

        final Tokenizer source = new StandardTokenizer();
        TokenStream result = new StopFilter(source, hyphenations);
        result = new ElisionFilter(result, articles);
        result = new IrishLowerCaseFilter(result);
        if (stopWordsEnabled) {
            result = new StopFilter(result, getStopWords());
        }
        if (stemExclusionsEnabled) {
            result = new SetKeywordMarkerFilter(result, getStemExclusions());
        }
        if (stemmingEnabled) {
            result = new SnowballFilter(result, new IrishStemmer());
        }
        return new TokenStreamComponents(source, result);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return IrishAnalyzer.getDefaultStopSet();
    }
}
