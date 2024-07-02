package datawave.query.language.analyzers.lucene;

import java.io.IOException;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.DutchStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Dutch language
 */
public class DutchLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected DutchLuceneAnalyzer() {
        super(DutchAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    protected TokenStream getStemFilter(TokenStream stream) {
        // Dutch has a fun little stem dictionary used in addition to the DutchStemmer
        stream = new StemmerOverrideFilter(stream, getStemDictionary());
        return super.getStemFilter(stream);
    }

    @Override
    protected SnowballProgram getStemmer() {
        return new DutchStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return DutchAnalyzer.getDefaultStopSet();
    }

    private StemmerOverrideFilter.StemmerOverrideMap getStemDictionary() {
        StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(false);
        builder.add("fiets", "fiets"); // otherwise fiet
        builder.add("bromfiets", "bromfiets"); // otherwise bromfiet
        builder.add("ei", "eier");
        builder.add("kind", "kinder");
        try {
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException("could not build stem dictionary", e);
        }
    }
}
