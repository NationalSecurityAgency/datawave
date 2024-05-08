package datawave.query.jexl.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;

class JexlQueryGeneratorTest {

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
        generateFixedSize(generator, 1, 1);
    }

    @Test
    void testNegations() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableNegations();
        generateFixedSize(generator, 10, 1);
    }

    @Test
    void testRegexes() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableRegexes();
        generateFixedSize(generator, 10, 1);
    }

    @Test
    void testFilterFunctions() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableFilterFunctions();
        generateFixedSize(generator, 10, 1);
    }

    @Test
    void testContentFunctions() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableContentFunctions();
        generateFixedSize(generator, 10, 1);
    }

    @Test
    void testGroupingFunctions() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableGroupingFunctions();
        generateFixedSize(generator, 10, 1);
    }

    @Test
    void testLargeJunction() {
        Set<String> fields = Collections.singleton("FIELD");
        Set<String> values = Collections.singleton("value");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableAllOptions();
        generateFixedSize(generator, 10, 5);
    }

    @Test
    void testHighCount() {
        Set<String> fields = Sets.newHashSet("F1", "F2", "F3");
        Set<String> values = Sets.newHashSet("v", "bar", "foo");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableAllOptions();
        generateFixedSize(generator, 15, 20);
    }

    @Test
    void testVariableQuerySize() {
        Set<String> fields = Sets.newHashSet("F1", "F2", "F3", "$11", "$22");
        Set<String> values = Sets.newHashSet("v", "bar", "foo");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).enableAllOptions();
        generateVariableSize(generator, 1500, 1, 15);
    }

    @Test
    void testGenerateUnions() {
        Set<String> fields = Collections.singleton("F");
        Set<String> values = Sets.newHashSet("1", "2", "3", "4");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).disableIntersections();
        generateVariableSize(generator, 10, 3, 7);
    }

    @Test
    void testGenerateIntersections() {
        Set<String> fields = Collections.singleton("F");
        Set<String> values = Sets.newHashSet("1", "2", "3", "4");
        QueryGenerator generator = new JexlQueryGenerator(fields, values).disableUnions();
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
        try {
            assertTrue(validator.isValid(JexlASTHelper.parseAndFlattenJexlQuery(query)));
        } catch (InvalidQueryTreeException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

}
