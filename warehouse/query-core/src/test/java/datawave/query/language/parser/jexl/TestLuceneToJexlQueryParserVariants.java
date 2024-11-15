package datawave.query.language.parser.jexl;

import com.google.common.collect.Sets;
import datawave.ingest.data.tokenize.DefaultTokenSearch;
import datawave.ingest.data.tokenize.StandardAnalyzer;
import datawave.ingest.data.tokenize.TokenSearch;
import datawave.query.language.parser.ParseException;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.tree.ServerHeadNode;
import org.apache.lucene.analysis.CharArraySet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestLuceneToJexlQueryParserVariants {
    private LuceneToJexlQueryParser parser;

    @Before
    public void setUp() {
        CharArraySet stopwords = new CharArraySet(1, true);
        stopwords.add("STOP");

        // TokenSearch is used for ingesting variants, and generally should never be used at query time
        // but is good for simulating the case where we want variants at query time.
        TokenSearch tokenSearch = TokenSearch.Factory.newInstance(DefaultTokenSearch.class.getName(), stopwords);
        StandardAnalyzer analyzer = new StandardAnalyzer(tokenSearch);
        parser = new LuceneToJexlQueryParser();
        parser.setSkipTokenizeUnfieldedFields(Sets.newHashSet("noToken"));
        parser.setTokenizedFields(Sets.newHashSet("tokField"));
        parser.setAnalyzer(analyzer);
    }

    @Test
    public void testVariantSingleTerm() throws ParseException {
        assertEquals("(TOKFIELD == 'foo@bar.com' || TOKFIELD == '@bar.com' || TOKFIELD == 'foo')", parseQuery("TOKFIELD:foo@bar.com"));
    }

    @Test
    public void testVariantStopword() throws ParseException {
        String expected = "("
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', 'STOP', 'foo@bar.com', 'baz') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', '@bar.com', 'baz') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', 'foo', 'baz') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', 'foo@bar.com', 'baz')"
                + ")";
        assertEquals(expected, parseQuery("TOKFIELD:\"email STOP foo@bar.com baz\""));
    }

    @Test
    public void testVariantSlopStopword() throws ParseException {
        // the split file `wi-fi` increases the slop
        String expected = "("
                + "content:within(TOKFIELD, 6, termOffsetMap, 'email', 'STOP', 'foo@bar.com', 'wi-fi') || "
                + "content:within(TOKFIELD, 7, termOffsetMap, 'email', '@bar.com', 'wi', 'fi') || "
                + "content:within(TOKFIELD, 7, termOffsetMap, 'email', 'foo', 'wi', 'fi') || "
                + "content:within(TOKFIELD, 7, termOffsetMap, 'email', 'foo@bar.com', 'wi', 'fi')"
                + ")";
        assertEquals(expected, parseQuery("TOKFIELD:\"email STOP foo@bar.com wi-fi\"~6"));
    }

    @Test
    public void testVariantsEnd() throws ParseException {
        String expected = "("
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', 'to', 'address', 'foo@bar.com') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', 'to', 'address', '@bar.com') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', 'to', 'address', 'foo'))";
        assertEquals(expected, parseQuery("TOKFIELD:\"email to address foo@bar.com\""));
    }

    @Test
    public void testVariantsBegin() throws ParseException {
        String expected = "("
                + "content:phrase(TOKFIELD, termOffsetMap, 'foo@bar.com', 'email', 'from', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, '@bar.com', 'email', 'from', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'foo', 'email', 'from', 'address'))";

        assertEquals(expected, parseQuery("TOKFIELD:\"foo@bar.com email from address\""));
    }

    @Test
    public void testVariantsMiddle() throws ParseException {
        String expected = "("
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', 'from', 'foo@bar.com', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', 'from', '@bar.com', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'email', 'from', 'foo', 'address'))";
        assertEquals(expected, parseQuery("TOKFIELD:\"email from foo@bar.com address\""));
    }

    @Test
    public void testVariantsMultiple() throws ParseException {
        String expected = "("
                + "content:phrase(TOKFIELD, termOffsetMap, 'from', 'foo@bar.com', 'to', 'bar@foo.com', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'from', '@bar.com', 'to', '@foo.com', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'from', '@bar.com', 'to', 'bar', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'from', '@bar.com', 'to', 'bar@foo.com', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'from', 'foo', 'to', '@foo.com', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'from', 'foo', 'to', 'bar', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'from', 'foo', 'to', 'bar@foo.com', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'from', 'foo@bar.com', 'to', '@foo.com', 'address') || "
                + "content:phrase(TOKFIELD, termOffsetMap, 'from', 'foo@bar.com', 'to', 'bar', 'address')"
                + ")";
        assertEquals(expected, parseQuery("TOKFIELD:\"from foo@bar.com to bar@foo.com address\""));
    }

    private String parseQuery(String query) throws ParseException {
        String parsedQuery = null;
        try {
            QueryNode node = parser.parse(query);
            if (node instanceof ServerHeadNode) {
                parsedQuery = node.getOriginalQuery();
            }
        } catch (RuntimeException e) {
            throw new ParseException(e);
        }
        return parsedQuery;
    }
}
