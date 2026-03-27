package datawave.query.transformer.annotation;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.query.parser.JavaRegexAnalyzer;

public class TermExtractorTest {
    private TermExtractor termExtractor;
    private Normalizer<String> normalizer;

    @BeforeEach
    public void setup() {
        termExtractor = new TermExtractor(null);
        normalizer = new LcNoDiacriticsNormalizer();
    }

    // test term extraction of eq nodes
    // @formatter:off
    @CsvSource({
            "UUID=='abc',abc",
            "UUID=='abc' || UUID=='def',abc;def",
            "UUID='a',",
            "UUID=='a' || UUID=='b' && UUID=='c',a;b;c;",
            "(UUID=='a' || UUID=='b') && (UUID=='c'),a;b;c",
            "(((UUID=='a'))),a"
    })
    // @formatter:on
    @ParameterizedTest(name = "extract {0}")
    public void extractEQTest(String query, String terms) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        test(query, terms);
    }

    // test term extraction of er nodes
    // @formatter:off
    @CsvSource({
            "UUID=~'abc',abc",
            "UUID=~'abc' || UUID=='def',abc;def",
            "UUID=~'a' || UUID=~'b' && UUID=~'c',a;b;c;",
            "(UUID=~'a' || UUID=~'b') && (UUID=~'c'),a;b;c",
            "(((UUID=~'a'))),a",
            "UUID=~'a.*',a.*",
            "UUID=~'.*a',.*a"
    })
    // @formatter:on
    @ParameterizedTest(name = "extract {0}")
    public void extractERTest(String query, String terms) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        test(query, terms);
    }

    // test term extraction of negated nodes
    // @formatter:off
    @CsvSource({
            "!(UUID=~'abc'),",
            "!(UUID=~'abc' || UUID=='def'),",
            "UUID=~'a' || !(UUID=~'b' && UUID=~'c'),a",
            "!(UUID=~'a' || UUID=~'b') && (UUID=~'c'),c",
            "!(!((UUID=~'a'))),a",
            "!(!(!(UUID=~'a'))),",
            "UUID!='abc',",
            "!UUID=='abc',",
            "!(!(UUID=='abc')),abc",
            "!(!(UUID=='abc')||(UUID=='def')),abc",
            "!(UUID!='abc'),abc"
    })
    // @formatter:on
    @ParameterizedTest(name = "extract {0}")
    public void extractNotTest(String query, String terms) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        test(query, terms);
    }

    // test term extraction with limited fields
    // @formatter:off
    @CsvSource({
            "UUID=='abc',",
            "UUID=~'abc.*',",
            "A=='abc',abc",
            "A=~'a.*',a.*",
            "B=='abc',abc",
            "B=~'b.*',b.*",
            "C=='abc',abc",
            "C=~'c.*',c.*",
            "D=='abc',",
            "D=~'d.*',"
    })
    // @formatter:on
    @ParameterizedTest(name = "extract {0}")
    public void extractTargetFieldsTest(String query, String terms) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        termExtractor = new TermExtractor(Set.of("A", "B", "C"));
        test(query, terms);
    }

    // test term extraction with case-sensitive limited fields
    // @formatter:off
    @CsvSource({
        "a=='abc',abc",
        "b=='abc',abc",
        "c=='abc',abc",
        "A=='abc',abc",
        "B=='abc',abc",
        "C=='abc',abc",
    })
    // @formatter:on
    @ParameterizedTest(name = "extract {0}")
    public void extractTargetCaseSensitiveFieldsTest(String query, String terms) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        termExtractor = new TermExtractor(Set.of("A", "B", "C"));
        test(query, terms);
        termExtractor = new TermExtractor(Set.of("a", "b", "c"));
        test(query, terms);
    }

    // test normalization is applied
    // @formatter:on
    @CsvSource({"UUID=='AbC',abc", "UUID=='ABC' || UUID=='dEf',abc;def", "UUID='A',", "UUID=='A' || UUID=='B' && UUID=='C',a;b;c;", "UUID=~'.*ABC.*',.*abc.*"})
    // @formatter:off
    @ParameterizedTest(name = "extract {0}")
    public void extractNormalizedTest(String query, String terms) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        test(query, terms);
    }

    // test non-jexl string
    @Test
    public void testNonJexl() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        assertThrows(IllegalArgumentException.class, () -> {
            termExtractor.extract("A:bc", normalizer);
        });
    }

    private void test(String query, String terms) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        Set<String> extracted = termExtractor.extract(query, normalizer);
        String[] splits;
        if (terms != null) {
            splits = terms.split(";");
        } else {
            splits = new String[0];
        }
        Set<String> expectedSet = new HashSet<>(Arrays.asList(splits));
        assertEquals(expectedSet, extracted);
    }
}
