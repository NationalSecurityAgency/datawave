package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;

/**
 * An example {@link LanguageAwareAnalyzer} for the English language
 */
public class EnglishLuceneAnalyzer extends LanguageAwareAnalyzer {

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        return new EnglishPossessiveFilter(stream);
    }

    @Override
    protected TokenStream getStopWordFilter(TokenStream stream) {
        // although query terms should already be lower cased, apply a lowercase filter to be safe
        stream = new LowerCaseFilter(stream);
        return new StopFilter(stream, stopwords);
    }

    @Override
    protected TokenStream getStemExclusions(TokenStream stream) {
        return new SetKeywordMarkerFilter(stream, stemExclusions);
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new PorterStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return EnglishAnalyzer.getDefaultStopSet();
    }
}
