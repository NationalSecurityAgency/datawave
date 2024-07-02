package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.BasqueStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Basque language
 */
public class BasqueLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected BasqueLuceneAnalyzer() {
        super(BasqueAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected SnowballProgram getStemmer() {
        return new BasqueStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return BasqueAnalyzer.getDefaultStopSet();
    }
}
