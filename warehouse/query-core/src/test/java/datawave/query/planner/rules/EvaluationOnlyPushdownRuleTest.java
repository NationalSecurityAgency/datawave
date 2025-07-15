package datawave.query.planner.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

public class EvaluationOnlyPushdownRuleTest {

    @Test
    public void testPushdown() {
        test("FIELD_A =~ 'abc.*'", "((_Eval_ = true) && (FIELD_A =~ 'abc.*'))");
        test("FIELD_B =~ 'xyz.*'", "((_Eval_ = true) && (FIELD_B =~ 'xyz.*'))");
    }

    @Test
    public void testNoPushdown() {
        // field does not match, regex does, rule not applied
        test("FIELD_D =~ 'abc.*'");
        test("FIELD_E =~ 'xyz.*'");

        // field matches, regex does not, rule not applied
        test("FIELD_A =~ '123.*'");
        test("FIELD_B =~ '456.*'");
    }

    private void test(String query) {
        test(query, query);
    }

    private void test(String query, String expected) {
        ASTJexlScript script = parse(query);
        NodeTransformVisitor visitor = getVisitor();

        JexlNode visited = (JexlNode) script.jjtAccept(visitor, null);

        String result = JexlStringBuildingVisitor.buildQuery(visited);
        assertEquals(expected, result);
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query, e);
            throw new RuntimeException(e);
        }
    }

    private NodeTransformVisitor getVisitor() {
        Set<FieldPattern> fieldPatterns = new HashSet<>();
        fieldPatterns.add(new FieldPattern("FIELD_A", "abc.*"));
        fieldPatterns.add(new FieldPattern("FIELD_B", "xyz.*"));

        EvaluationOnlyPushdownRule rule = new EvaluationOnlyPushdownRule();
        rule.setFieldPatterns(fieldPatterns);

        List<NodeTransformRule> rules = new ArrayList<>();
        rules.add(rule);

        return new NodeTransformVisitor(null, null, rules);
    }
}
