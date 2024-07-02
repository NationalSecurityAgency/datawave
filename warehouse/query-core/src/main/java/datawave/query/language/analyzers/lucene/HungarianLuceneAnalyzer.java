package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.HungarianStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Hungarian language
 */
public class HungarianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected HungarianLuceneAnalyzer() {
        super(HungarianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected SnowballProgram getStemmer() {
        return new HungarianStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return HungarianAnalyzer.getDefaultStopSet();
    }
}
