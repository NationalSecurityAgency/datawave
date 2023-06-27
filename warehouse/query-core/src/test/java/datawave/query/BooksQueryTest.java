package datawave.query;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.OR_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.DocumentPermutation;
import datawave.query.testframework.AbstractFields;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.BooksDataManager;
import datawave.query.testframework.BooksDataType;
import datawave.query.testframework.BooksDataType.BooksEntry;
import datawave.query.testframework.BooksDataType.BooksField;
import datawave.query.testframework.ConfigData;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.RawDataManager;

/**
 * Tests for grouping data. The {@link BooksDataManager} will create the valid grouping entries.
 */
public class BooksQueryTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(BooksQueryTest.class);

    private static final Map<String,String> QUERY_OPTIONS = ImmutableMap.of(QueryParameters.INCLUDE_GROUPING_CONTEXT, Boolean.TRUE.toString());
    private static final RawDataManager BOOKS_MANAGER = createManager();

    private static RawDataManager createManager() {
        try {
            Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
            FieldConfig indexes = new BooksFieldIndex();
            ConfigData cfgData = new ConfigData(BooksField.BOOKS_DATE.name(), BooksField.ISBN_13.name(), BooksField.getHeaders(),
                            BooksDataType.getDefaultVisibility(), BooksField.getFieldsMetadata());

            DataTypeHadoopConfig books = new BooksDataType(BooksEntry.tech.getDataType(), BooksEntry.tech.getIngestFile(), indexes, cfgData);
            dataTypes.add(books);

            accumuloSetup.setData(FileType.GROUPING, dataTypes);
            client = accumuloSetup.loadTables(log);
            BooksDataManager mgr = new BooksDataManager(BooksEntry.tech.getDataType(), client, indexes, cfgData);
            mgr.loadGroupingData(books.getIngestFile());
            return mgr;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public BooksQueryTest() {
        super(BOOKS_MANAGER);
    }

    @Test
    public void testLanguage() throws Exception {
        log.info("------  testLanguage  ------");
        String[] languages = {"'FrEnch'", "'enGLIsh'", "'GerMan'", "'Greek'"};
        for (String lang : languages) {
            String query = BooksField.LANGUAGE.name() + EQ_OP + lang;
            runTest(query, query, QUERY_OPTIONS);
        }
    }

    @Test
    public void testAuthor() throws Exception {
        log.info("------  testAuthor  ------");
        String[] authors = {"'douG Lea'", "'Joshua Bloch'"};
        for (String auth : authors) {
            String query = BooksField.AUTHOR.name() + EQ_OP + auth;
            runTest(query, query, QUERY_OPTIONS);
        }
    }

    @Test
    public void testMultiAuthorOr() throws Exception {
        log.info("------  testMultiAuthor  ------");
        String doug = "'douG Lea'";
        String joshua = "'NeaL GafTEr'";
        String query = BooksField.AUTHOR.name() + EQ_OP + doug + OR_OP + BooksField.AUTHOR.name() + EQ_OP + joshua;
        runTest(query, query, QUERY_OPTIONS);
    }

    @Test
    public void testEvaluationOnlyAuthor() throws Exception {
        log.info("------  testEvaluationOnlyAuthor  ------");
        String bloch = "BLOCH";
        logic.setEvaluationOnlyFields(AUTHOR_FIRST_NAME + ',' + AUTHOR_LAST_NAME);
        logic.setDocumentPermutations(Collections.singletonList(AuthorNameParts.class.getName()));
        String query = AUTHOR_LAST_NAME + EQ_OP + '\'' + bloch + '\'' + AND_OP + BooksField.LANGUAGE.name() + EQ_OP + "'ENGLISH'";
        String expectedQuery = BooksField.AUTHOR + RE_OP + "'.*" + bloch + '\'' + AND_OP + BooksField.LANGUAGE.name() + EQ_OP + "'ENGLISH'";
        runTest(query, expectedQuery, QUERY_OPTIONS);
    }

    public static String AUTHOR_LAST_NAME = "AUTHOR_LAST_NAME";
    public static String AUTHOR_FIRST_NAME = "AUTHOR_FIRST_NAME";

    public static class AuthorNameParts implements DocumentPermutation {

        @Nullable
        @Override
        public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> keyDocumentEntry) {
            if (keyDocumentEntry != null) {
                Document doc = keyDocumentEntry.getValue();
                if (doc != null) {
                    Map<String,Attribute<? extends Comparable<?>>> attrs = new HashMap<>();
                    for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : doc.getDictionary().entrySet()) {
                        if (entry.getKey().equals(BooksField.AUTHOR.name()) || entry.getKey().startsWith(BooksField.AUTHOR.name() + '.')) {
                            String attrExtra = entry.getKey().substring(BooksField.AUTHOR.name().length());
                            Attribute attr = entry.getValue();
                            String name = attr.getData().toString();
                            String[] parts = StringUtils.split(name, ' ');
                            if (parts.length > 1) {
                                attrs.put(AUTHOR_FIRST_NAME + attrExtra, new Content(parts[0], attr.getMetadata(), false));
                            }
                            attrs.put(AUTHOR_LAST_NAME + attrExtra, new Content(parts[parts.length - 1], attr.getMetadata(), false));
                        }
                    }
                    for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : attrs.entrySet()) {
                        doc.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            return keyDocumentEntry;
        }
    }

    @Test
    public void testAuthorAndISBN_10() throws Exception {
        log.info("------  testISBN_10  ------");
        String[] isbn_10 = {"'0-321-33678-X'", "'0-321-34960-1'"};
        String auth = "'JoShua BLOCH'";
        for (String isbn : isbn_10) {
            String query = BooksField.AUTHOR.name() + EQ_OP + auth + AND_OP + BooksField.ISBN_10.name() + EQ_OP + isbn;
            runTest(query, query, QUERY_OPTIONS);
        }
    }

    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = BooksDataType.getTestAuths();
        // add suffix for grouping
        this.documentKey = BooksField.ISBN_13.name() + ".0";
    }

    private static class BooksFieldIndex extends AbstractFields {

        private static final Collection<String> index = new HashSet<>();
        private static final Collection<String> indexOnly = new HashSet<>();
        private static final Collection<String> reverse = new HashSet<>();
        private static final Collection<String> multivalue = new HashSet<>();
        private static final Collection<Set<String>> composite = new HashSet<>();
        private static final Collection<Set<String>> virtual = new HashSet<>();

        static {
            index.add(BooksField.TITLE.name());
            index.add(BooksField.AUTHOR.name());
            index.add(BooksField.LANGUAGE.name());
            reverse.addAll(index);
        }

        public BooksFieldIndex() {
            super(index, indexOnly, reverse, multivalue, composite, virtual);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{" + super.toString() + "}";
        }
    }
}
