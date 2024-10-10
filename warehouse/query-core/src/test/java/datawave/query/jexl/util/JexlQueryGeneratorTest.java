package datawave.query.jexl.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import datawave.core.query.jexl.visitors.validate.ASTValidator;
import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;

class JexlQueryGeneratorTest {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(JexlQueryGeneratorTest.class);

    private final int maxIterations = 1_000;
    private final int size = 1;
    private final int minSize = 2;
    private final int maxSize = 15;
    private final ASTValidator validator = new ASTValidator();

    @BeforeEach
    void beforeEach() {
        Logger.getLogger(ASTValidator.class).setLevel(Level.OFF);
    }

    @Test
    void testSimpleQuery() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableAllOptions();
        generateQueries(generator);
    }

    @Test
    void testNegations() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableNegations();
        generateQueries(generator);
    }

    @Test
    void testRegexes() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableRegexes();
        generateQueries(generator);
    }

    @Test
    void testFilterFunctions() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableFilterFunctions();
        testFunctions(generator);
    }

    @Test
    void testContentFunctions() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableContentFunctions();
        testFunctions(generator);
    }

    @Test
    void testGroupingFunctions() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableGroupingFunctions();
        testFunctions(generator);
    }

    @Test
    void testLargeJunction() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableAllOptions();
        generateFixedSize(generator, maxIterations, maxSize);
    }

    @Test
    void testHighCount() {
        Set<String> fields = Sets.newHashSet("F1", "F2", "F3");
        Set<String> values = Sets.newHashSet("v", "bar", "foo");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableAllOptions();
        generateFixedSize(generator, 100, 100);
    }

    @Test
    void testVariableQuerySize() {
        Set<String> fields = Sets.newHashSet("F1", "F2", "F3", "$11", "$22");
        Set<String> values = Sets.newHashSet("v", "bar", "foo");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableAllOptions();
        generateVariableSize(generator, 1500, minSize, maxSize);
    }

    @Test
    void testGenerateUnions() {
        Set<String> fields = Collections.singleton("F");
        Set<String> values = Sets.newHashSet("1", "2", "3", "4");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).disableIntersections();
        generateVariableSize(generator, maxIterations, minSize, maxSize);
    }

    @Test
    void testGenerateIntersections() {
        Set<String> fields = Collections.singleton("F");
        Set<String> values = Sets.newHashSet("1", "2", "3", "4");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).disableUnions();
        generateQueries(generator);
    }

    private void testFunctions(QueryGenerator generator) {
        generateFixedSize(generator, maxIterations, size);
        generateVariableSize(generator, maxIterations, minSize, maxSize);

        generator.enableNoFieldedFunctions();
        generateFixedSize(generator, maxIterations, size);
        generateVariableSize(generator, maxIterations, minSize, maxSize);

        generator.disableNoFieldedFunctions();
        generator.enableMultiFieldedFunctions();
        generateFixedSize(generator, maxIterations, size);
        generateVariableSize(generator, maxIterations, minSize, maxSize);
    }

    private void generateQueries(QueryGenerator generator) {
        generateFixedSize(generator, maxIterations, size);
        generateVariableSize(generator, maxIterations, minSize, maxSize);
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
        try {
            assertTrue(validator.isValid(JexlASTHelper.parseAndFlattenJexlQuery(query)));
        } catch (InvalidQueryTreeException | ParseException e) {
            log.info("Failed to parse query: {}", query);
            throw new RuntimeException(e);
        }
    }

}
