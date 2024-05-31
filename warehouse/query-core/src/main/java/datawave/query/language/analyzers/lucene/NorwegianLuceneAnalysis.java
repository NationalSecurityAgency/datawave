package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.NorwegianStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Norwegian language
 */
public class NorwegianLuceneAnalysis extends LanguageAwareAnalyzer {

    protected NorwegianLuceneAnalysis() {
        super(NorwegianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    protected SnowballProgram getStemmer() {
        return new NorwegianStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return NorwegianAnalyzer.getDefaultStopSet();
    }
}
