package datawave.query.jexl.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.core.query.jexl.visitors.validate.ASTValidator;
import datawave.core.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.jexl.JexlASTHelper;

class LuceneQueryGeneratorTest {

    private final ASTValidator validator = new ASTValidator();

    @BeforeEach
    void beforeEach() {
        Logger.getLogger(ASTValidator.class).setLevel(Level.OFF);
    }

    @Test
    void testSimpleQuery() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new LuceneQueryGenerator(fields, values);
        generateFixedSize(generator, 5, 1);
    }

    @Test
    void testNegations() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new LuceneQueryGenerator(fields, values).enableNegations();
        generateFixedSize(generator, 5, 2);
        generateFixedSize(generator, 5, 5);
    }

    @Test
    void testRegexes() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new LuceneQueryGenerator(fields, values).enableRegexes();
        generateFixedSize(generator, 10, 1);
    }

    @Test
    void testFilterFunctions() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new LuceneQueryGenerator(fields, values).enableFilterFunctions();
        generateFixedSize(generator, 10, 1);
    }

    @Test
    void testContentFunctions() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new LuceneQueryGenerator(fields, values).enableContentFunctions();
        generateFixedSize(generator, 20, 1);
    }

    @Test
    void testGroupingFunctions() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new LuceneQueryGenerator(fields, values).enableGroupingFunctions();
        generateFixedSize(generator, 10, 1);
    }

    @Test
    void testVariableSizedQueries() {
        Set<String> fields = Sets.newHashSet("F1", "F2", "F3", "$11", "$22");
        Set<String> values = Sets.newHashSet("v", "var", "foo");
        QueryGenerator generator = new LuceneQueryGenerator(fields, values).enableRegexes();
        generateVariableSize(generator, 1000, 1, 10);
    }

    @Test
    void testCases() {
        validateQuery("(FIELD:value NOT FIELD:value)");
        validateQuery("(FIELD:value FIELD:value NOT FIELD:value)");
    }

    @Test
    void testGenerateUnions() {
        Set<String> fields = Collections.singleton("F");
        Set<String> values = Sets.newHashSet("1", "2", "3", "4");
        QueryGenerator generator = new LuceneQueryGenerator(fields, values).disableIntersections();
        generateVariableSize(generator, 10, 3, 7);
    }

    @Test
    void testGenerateIntersections() {
        Set<String> fields = Collections.singleton("F");
        Set<String> values = Sets.newHashSet("1", "2", "3", "4");
        QueryGenerator generator = new LuceneQueryGenerator(fields, values).disableUnions();
        generateVariableSize(generator, 10, 3, 7);
    }

    private void generateFixedSize(QueryGenerator generator, int maxIterations, int size) {
        for (int i = 0; i < maxIterations; i++) {
            String query = generator.getQuery(size);
            validateQuery(query);
        }
    }

    private void generateVariableSize(QueryGenerator generator, int maxIterations, int min, int max) {
        for (int i = 0; i < maxIterations; i++) {
            String query = generator.getQuery(min, max);
            validateQuery(query);
        }
    }

    private void validateQuery(String query) {
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        try {
            String parsed = parser.parse(query).getOriginalQuery();
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(parsed);
            assertTrue(validator.isValid(script));
        } catch (Exception e) {
            Assertions.fail("Failed to validate query: " + query);
            throw new RuntimeException(e);
        }
    }
}
