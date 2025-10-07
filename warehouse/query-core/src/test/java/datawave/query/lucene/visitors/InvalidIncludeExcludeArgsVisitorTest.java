package datawave.query.lucene.visitors;

import static datawave.query.lucene.visitors.InvalidIncludeExcludeArgsVisitor.REASON.NO_ARGS_AFTER_BOOLEAN;
import static datawave.query.lucene.visitors.InvalidIncludeExcludeArgsVisitor.REASON.UNEVEN_ARGS;
import static datawave.query.lucene.visitors.InvalidIncludeExcludeArgsVisitor.REASON.UNEVEN_ARGS_AFTER_BOOLEAN;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;

public class InvalidIncludeExcludeArgsVisitorTest {

    private static final SyntaxParser parser = new AccumuloSyntaxParser();
    private String query;
    private final List<InvalidIncludeExcludeArgsVisitor.InvalidFunction> expected = new ArrayList<>();

    @AfterEach
    public void tearDown() throws Exception {
        query = null;
        expected.clear();
    }

    /**
     * Test a query that does not have the INCLUDE or EXCLUDE function.
     */
    @Test
    public void testQueryWithNoIncludeOrExcludeFunction() throws QueryNodeParseException {
        givenQuery("FOO:'abc'");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single field and value.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithSingleFieldAndValue(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(FOO,'abc')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single field and value.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithMultipleFieldAndValue(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(FOO,'abc',BAR,'def')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single field and value after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithSingleFieldAndValueAfterOR(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(OR,FOO,'abc')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single field and value after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithSingleFieldAndValueAfterAND(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(AND,FOO,'abc')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with multiple fields and values after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithMultipleFieldsAndValuesAfterOR(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(OR,FOO,'abc',BAR,'def')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with multiple fields and values after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    public void testFunctionWithMultipleFieldsAndValuesAfterAND(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(AND,FOO,'abc',BAR,'def')");
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with just a single arg.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithSingleArg(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(FIELD)");
        expect(new InvalidIncludeExcludeArgsVisitor.InvalidFunction(name, List.of("FIELD"), UNEVEN_ARGS));
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with an uneven number of arguments greater than one without a boolean arg.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithUnevenArgs(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(FIELD1,'value',FIELD2)");
        expect(new InvalidIncludeExcludeArgsVisitor.InvalidFunction(name, List.of("FIELD1", "value", "FIELD2"), UNEVEN_ARGS));
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with no arguments after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithNoArgsAfterOR(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(OR)");
        expect(new InvalidIncludeExcludeArgsVisitor.InvalidFunction(name, List.of("OR"), NO_ARGS_AFTER_BOOLEAN));
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with no arguments after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithNoArgsAfterAND(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(AND)");
        expect(new InvalidIncludeExcludeArgsVisitor.InvalidFunction(name, List.of("AND"), NO_ARGS_AFTER_BOOLEAN));
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single argument after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithSingleArgsAfterOR(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(OR,'value')");
        expect(new InvalidIncludeExcludeArgsVisitor.InvalidFunction(name, List.of("OR", "value"), UNEVEN_ARGS_AFTER_BOOLEAN));
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with a single argument after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithSingleArgsAfterAND(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(AND,'value')");
        expect(new InvalidIncludeExcludeArgsVisitor.InvalidFunction(name, List.of("AND", "value"), UNEVEN_ARGS_AFTER_BOOLEAN));
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with an uneven number of arguments greater than one after an OR boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithUnevenArgsAfterOR(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(OR,FIELD1,'value',FIELD2)");
        expect(new InvalidIncludeExcludeArgsVisitor.InvalidFunction(name, List.of("OR", "FIELD1", "value", "FIELD2"), UNEVEN_ARGS_AFTER_BOOLEAN));
        assertResult();
    }

    /**
     * Test versions of the INCLUDE and EXCLUDE functions with an uneven number of arguments greater than one after an AND boolean.
     *
     * @param name
     *            the function name
     */
    @ParameterizedTest
    @ValueSource(strings = {"INCLUDE", "EXCLUDE"})
    void testFunctionWithUnevenArgsAfterAND(String name) throws QueryNodeParseException {
        givenQuery("#" + name + "(AND,FIELD1,'value',FIELD2)");
        expect(new InvalidIncludeExcludeArgsVisitor.InvalidFunction(name, List.of("AND", "FIELD1", "value", "FIELD2"), UNEVEN_ARGS_AFTER_BOOLEAN));
        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expect(InvalidIncludeExcludeArgsVisitor.InvalidFunction function) {
        this.expected.add(function);
    }

    private void assertResult() throws QueryNodeParseException {
        QueryNode queryNode = parser.parse(query, "");
        List<InvalidIncludeExcludeArgsVisitor.InvalidFunction> actual = InvalidIncludeExcludeArgsVisitor.check(queryNode);
        Assertions.assertEquals(expected, actual);
    }
}
