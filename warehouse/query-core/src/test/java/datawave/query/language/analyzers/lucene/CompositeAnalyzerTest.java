package datawave.query.language.analyzers.lucene;

import java.io.IOException;

import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.ingest.data.tokenize.StandardAnalyzer;
import datawave.query.exceptions.DatawaveFatalQueryException;

public class CompositeAnalyzerTest {

    private final String text = "My brother's company is A.C.M.E. Inc.";

    private final StandardAnalyzer standardAnalyzer = new StandardAnalyzer();

    private final EnglishAnalyzer englishAnalyzer = new EnglishAnalyzer();

    @Test
    public void initialAssertions() {
        String standardTokens = readTokens(standardAnalyzer);
        String englishTokens = readTokens(englishAnalyzer);

        String expectedStandardTokens = "my brother company acme inc";
        String expectedEnglishTokens = "my brother compani a.c.m. inc";

        Assertions.assertEquals(expectedStandardTokens, standardTokens);
        Assertions.assertEquals(expectedEnglishTokens, englishTokens);
    }

    private String readTokens(StopwordAnalyzerBase base) {
        try (TokenStream tokenStream = base.tokenStream("FIELD", text)) {
            tokenStream.reset();

            StringBuilder sb = new StringBuilder();
            while (tokenStream.incrementToken()) {
                String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
                if (token != null) {
                    sb.append(token).append(" ");
                }
            }
            sb.setLength(sb.length() - 1); // trim trailing space

            return sb.toString();
        } catch (Exception e) {
            throw new DatawaveFatalQueryException(e);
        }
    }

    private String compositeStream() {
        try (TokenStream standardStream = standardAnalyzer.tokenStream("FIELD", text)) {

            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
