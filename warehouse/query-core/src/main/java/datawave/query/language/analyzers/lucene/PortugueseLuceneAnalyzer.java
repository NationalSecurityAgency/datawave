package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseLightStemFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the Portuguese language
 */
public class PortugueseLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected PortugueseLuceneAnalyzer() {
        super(PortugueseAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new PortugueseLightStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return PortugueseAnalyzer.getDefaultStopSet();
    }
}
