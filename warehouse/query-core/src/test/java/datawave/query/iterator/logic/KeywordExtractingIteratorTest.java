package datawave.query.iterator.logic;

import static datawave.query.iterator.logic.KeywordExtractingIterator.DEFAULT_VIEW_NAMES;
import static datawave.query.iterator.logic.KeywordExtractingIterator.DOCUMENT_LANGUAGES;
import static datawave.query.iterator.logic.KeywordExtractingIterator.VIEW_NAMES;
import static datawave.query.tables.keyword.KeywordQueryLogic.ALL;
import static datawave.query.tables.keyword.KeywordQueryLogic.PARENT_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.gson.Gson;

import datawave.query.Constants;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.util.keyword.KeywordExtractor;
import datawave.util.keyword.KeywordResults;
import datawave.util.keyword.VisibleContent;

@RunWith(EasyMockRunner.class)
public class KeywordExtractingIteratorTest extends EasyMockSupport {

    private static final Text row = new Text("20220115_1");
    private static final Text colf = Constants.D_COLUMN_FAMILY;

    @Mock
    private IteratorEnvironment env;
    private static final List<Map.Entry<Key,Value>> source = new ArrayList<>();
    private final Map<String,String> options = new HashMap<>();
    private final KeywordExtractingIterator iterator = new KeywordExtractingIterator();

    private static final String email123456789Content1 = "test empty results";
    private static final String email987654321Content1 = "as the amount of generated information grows reading and summarizing texts of large collections turns into a challenging task many documents do not come with descriptive terms thus requiring humans to generate keywords on-the-fly the need to automate this kind of task demands the development of keyword extraction systems with the ability to automatically identify keywords within the text one approach is to resort to machine-learning algorithms these however depend on large annotated text corpora which are not always available an alternative solution is to consider an unsupervised approach in this article we describe yake a light-weight unsupervised automatic keyword extraction method which rests on statistical text features extracted from single documents to select the most relevant keywords of a text our system does not need to be trained on a particular set of documents nor does it depend on dictionaries external corpora text size language or domain to demonstrate the merits and significance of yake we compare it against ten state-of-the-art unsupervised approaches and one supervised method experimental results carried out on top of twenty datasets show that yake significantly outperforms other unsupervised methods on texts of different sizes languages and domains";
    private static final String email987654321Content2 = "The remainder of this article is structured as follows Section 2 offers a comprehensive overview of related research Section 3 defines the architecture of YAKE Section 4 describes the experimental setup Section 5 discusses the obtained results Section 6 provides a detailed analysis on feature importance Finally Section 7 summarizes the article and concludes with some final remarks";
    private static final String pdf111222333Content = "YAKE is a lightweight unsupervised automatic keyword extraction method that uses text statistical features to select the most important keywords from a document It requires no training external corpus or dictionaries and works across multiple languages and domains regardless of text size ";

    /**
     * Set up our data source before the tests begin.
     */
    @BeforeClass
    public static void beforeClass() throws IOException {
        givenData("email", "123.456.789", "CONTENT1", email123456789Content1);
        givenData("email", "987.654.321", "CONTENT1", email987654321Content1);
        givenData("email", "987.654.321", "CONTENT2", email987654321Content2);
        givenData("pdf", "111.222.333", "CONTENT", pdf111222333Content);
    }

    /**
     * For the data given to this function, add a key to the data source that contains the given information. <br>
     * For these tests, 'd' column keys are created with the specified data type, uid, content name, and the content to be gzipped and base64 encoded.
     */
    private static void givenData(String datatype, String uid, String contentName, String content) throws IOException {
        Text colq = new Text(datatype + Constants.NULL + uid + Constants.NULL + contentName);
        Key key = new Key(row, colf, colq, new ColumnVisibility("ALL"), new Date().getTime());
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final OutputStream b64s = Base64.getEncoder().wrap(bos);
        final GZIPOutputStream gzip = new GZIPOutputStream(b64s);
        gzip.write(content.getBytes());
        gzip.close();
        b64s.close();
        bos.close();
        Value value = new Value(bos.toByteArray());
        Map.Entry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);
        source.add(entry);
    }

    /**
     * Clear the iterator options after every test.
     */
    @After
    public void tearDown() {
        options.clear();
    }

    /**
     * Add the passed in values to their respective options to be used in the iterator for the test.
     */
    private void givenOptions(String viewNames, Map<String,String> languagemap) {
        options.put(VIEW_NAMES, viewNames);
        options.put(DOCUMENT_LANGUAGES, new Gson().toJson(languagemap));
    }

    /**
     * Initializes an iterator on the data source with the options set by calling {@code givenOptions()}
     */
    private void initIterator() throws IOException {
        iterator.init(new SortedListKeyValueIterator(source), options, env);
    }

    /**
     * We expect the iterator to not return anything when the specified content is not found.
     */
    @Test
    public void testNoContentFound() throws IOException {
        String uid = "i dont exist";
        String rowDtUid = row + "/email/" + uid;
        String dtUid = "email" + Constants.NULL + uid;
        List<String> viewNames = List.of("CONTENT1");

        Map<String,String> languagemap = new HashMap<>();
        languagemap.put(rowDtUid, "ENGLISH");
        givenOptions(String.join(Constants.COMMA, viewNames), languagemap);

        initIterator();

        Key startKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL));
        Key endKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + ALL));
        Range range = new Range(startKey, true, endKey, false);

        iterator.seek(range, Collections.emptyList(), false);

        assertFalse(iterator.hasTop());
    }

    /**
     * We expect the iterator to not return anything when the {@code KeywordExtractor} does not produce keywords for the found content.
     */
    @Test
    public void testNoKeywordsGenerated() throws IOException {
        String uid = "123.456.789";
        String rowDtUid = row + "/email/" + uid;
        String dtUid = "email" + Constants.NULL + uid;
        List<String> viewNames = List.of("CONTENT1");

        Map<String,String> languagemap = new HashMap<>();
        languagemap.put(rowDtUid, "ENGLISH");
        givenOptions(String.join(Constants.COMMA, viewNames), languagemap);

        initIterator();

        Key startKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL));
        Key endKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + ALL));
        Range range = new Range(startKey, true, endKey, false);

        iterator.seek(range, Collections.emptyList(), false);

        assertFalse(iterator.hasTop());
    }

    /**
     * Test that we find content when searching with the "all" range (TLD and all children).
     */
    @Test
    public void testMatchAll() throws IOException {
        String uid = "987.654.321";
        String rowDtUid = row + "/email/" + uid;
        String dtUid = "email" + Constants.NULL + uid;
        List<String> viewNames = List.of("CONTENT1");

        Map<String,String> languagemap = new HashMap<>();
        languagemap.put(rowDtUid, "ENGLISH");
        givenOptions(String.join(Constants.COMMA, viewNames), languagemap);

        Map<String,VisibleContent> expectedContentMap = Map.of("CONTENT1", new VisibleContent("ALL", email987654321Content1));
        KeywordExtractor expectedKeywordExtractor = new KeywordExtractor(rowDtUid, viewNames, expectedContentMap, "ENGLISH", options);

        initIterator();

        Key startKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL));
        Key endKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL + ALL));
        Range range = new Range(startKey, true, endKey, false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(Constants.D_COLUMN_FAMILY, topKey.getColumnFamily());
        assertTrue(topKey.getColumnQualifier().toString().startsWith(dtUid));
        assertEquals(expectedKeywordExtractor.extractKeywords().toJson(), KeywordResults.deserialize(iterator.getTopValue().get()).toJson());
    }

    /**
     * Test that we find content when using the range to only return the TLD ({@code PARENT_ONLY}).
     */
    @Test
    public void testMatchTLD() throws IOException {
        String uid = "987.654.321";
        String rowDtUid = row + "/email/" + uid;
        String dtUid = "email" + Constants.NULL + uid;
        List<String> viewNames = List.of("CONTENT1");

        Map<String,String> languagemap = new HashMap<>();
        languagemap.put(rowDtUid, "ENGLISH");
        givenOptions(String.join(Constants.COMMA, viewNames), languagemap);

        Map<String,VisibleContent> expectedContentMap = Map.of("CONTENT1", new VisibleContent("ALL", email987654321Content1));
        KeywordExtractor expectedKeywordExtractor = new KeywordExtractor(rowDtUid, viewNames, expectedContentMap, "ENGLISH", options);

        initIterator();

        Key startKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL));
        Key endKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + PARENT_ONLY));
        Range range = new Range(startKey, true, endKey, false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(Constants.D_COLUMN_FAMILY, topKey.getColumnFamily());
        assertTrue(topKey.getColumnQualifier().toString().startsWith(dtUid));
        assertEquals(expectedKeywordExtractor.extractKeywords().toJson(), KeywordResults.deserialize(iterator.getTopValue().get()).toJson());
    }

    /**
     * Test that the wild card matching works as expected.<br>
     * It should generate keywords for the first view found that matches the term when iterating through the keys.
     */
    @Test
    public void testMatchWildcard() throws IOException {
        String uid = "987.654.321";
        String rowDtUid = row + "/email/" + uid;
        String dtUid = "email" + Constants.NULL + uid;
        List<String> viewNames = List.of("CONTENT*");

        Map<String,String> languagemap = new HashMap<>();
        languagemap.put(rowDtUid, "ENGLISH");
        givenOptions(String.join(Constants.COMMA, viewNames), languagemap);

        Map<String,VisibleContent> expectedContentMap = Map.of("CONTENT1", new VisibleContent("ALL", email987654321Content1));
        KeywordExtractor expectedKeywordExtractor = new KeywordExtractor(rowDtUid, viewNames, expectedContentMap, "ENGLISH", options);

        initIterator();

        Key startKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL));
        Key endKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL + ALL));
        Range range = new Range(startKey, true, endKey, false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(Constants.D_COLUMN_FAMILY, topKey.getColumnFamily());
        assertTrue(topKey.getColumnQualifier().toString().startsWith(dtUid));
        assertEquals(expectedKeywordExtractor.extractKeywords().toJson(), KeywordResults.deserialize(iterator.getTopValue().get()).toJson());
    }

    /**
     * Test that we extract keywords based on the first matched content from the prioritized list.
     */
    @Test
    public void testPrioritizedList() throws IOException {
        String uid = "987.654.321";
        String rowDtUid = row + "/email/" + uid;
        String dtUid = "email" + Constants.NULL + uid;
        List<String> viewNames = List.of("CONTENT2", "CONTENT1");

        Map<String,String> languagemap = new HashMap<>();
        languagemap.put(rowDtUid, "ENGLISH");
        givenOptions(String.join(Constants.COMMA, viewNames), languagemap);

        Map<String,VisibleContent> expectedContentMap = Map.of("CONTENT2", new VisibleContent("ALL", email987654321Content2));
        KeywordExtractor expectedKeywordExtractor = new KeywordExtractor(rowDtUid, viewNames, expectedContentMap, "ENGLISH", options);

        initIterator();

        Key startKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL));
        Key endKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL + ALL));
        Range range = new Range(startKey, true, endKey, false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(Constants.D_COLUMN_FAMILY, topKey.getColumnFamily());
        assertTrue(topKey.getColumnQualifier().toString().startsWith(dtUid));
        assertEquals(expectedKeywordExtractor.extractKeywords().toJson(), KeywordResults.deserialize(iterator.getTopValue().get()).toJson());
    }

    /**
     * Test that if we do not specify our own list of prioritized views, that the default list will be used.
     */
    @Test
    public void testMatchDefaultContent() throws IOException {
        String uid = "111.222.333";
        String rowDtUid = row + "/pdf/" + uid;
        String dtUid = "pdf" + Constants.NULL + uid;

        Map<String,String> languagemap = new HashMap<>();
        languagemap.put(rowDtUid, "ENGLISH");
        options.put(DOCUMENT_LANGUAGES, new Gson().toJson(languagemap));

        Map<String,VisibleContent> expectedContentMap = Map.of("CONTENT", new VisibleContent("ALL", pdf111222333Content));
        KeywordExtractor expectedKeywordExtractor = new KeywordExtractor(rowDtUid, List.of(DEFAULT_VIEW_NAMES.split(Constants.COMMA)), expectedContentMap,
                        "ENGLISH", options);

        initIterator();

        Key startKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL));
        Key endKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtUid + Constants.NULL + ALL));
        Range range = new Range(startKey, true, endKey, false);

        iterator.seek(range, Collections.emptyList(), false);

        assertTrue(iterator.hasTop());

        Key topKey = iterator.getTopKey();
        assertEquals(row, topKey.getRow());
        assertEquals(Constants.D_COLUMN_FAMILY, topKey.getColumnFamily());
        assertTrue(topKey.getColumnQualifier().toString().startsWith(dtUid));
        assertEquals(expectedKeywordExtractor.extractKeywords().toJson(), KeywordResults.deserialize(iterator.getTopValue().get()).toJson());
    }
}
