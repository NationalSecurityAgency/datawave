package datawave.query.jexl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.jexl.visitors.DepthVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TermCountingVisitor;

class LargeJexlQueryTest {

    private static final int TERMS = 100_000;

    @ParameterizedTest
    @ValueSource(strings = {" || ", " && "})
    void parsesWideJunctionsWithoutAnIntermediateBinaryTree(String operator) throws Exception {
        String query = query(operator);
        assertWideJunction(JexlASTHelper.parseJexlQuery(query), operator);
        assertWideJunction(ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).parse(query), operator);
    }

    @ParameterizedTest
    @ValueSource(strings = {" || ", " && "})
    void flattensCountsAndSerializesLargeQueries(String operator) throws Exception {
        String query = query(operator);
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        assertWideJunction(script, operator);
        assertEquals(TERMS, TermCountingVisitor.countTerms(script));
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(script));
    }

    @Test
    void evaluatesAllOrOperandsWhenTheMatchIsLast() {
        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query(" || "));
        DatawaveJexlContext context = new DatawaveJexlContext();

        context.set("FIELD", Integer.toString(TERMS - 1));
        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
        context.set("FIELD", "absent");
        assertFalse(ArithmeticJexlEngines.isMatched(script.execute(context)));
    }

    @Test
    void evaluatesAllAndOperandsWithoutRecursiveJunctions() {
        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query(" && "));
        DatawaveJexlContext context = new DatawaveJexlContext();

        context.set("FIELD", "absent");
        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
        context.set("FIELD", Integer.toString(TERMS - 1));
        assertFalse(ArithmeticJexlEngines.isMatched(script.execute(context)));
    }

    @Test
    void preservesPrecedenceInLargeMixedQueries() throws Exception {
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < TERMS / 2; i++) {
            if (i > 0) {
                query.append(" || ");
            }
            query.append("FIELD == '").append(i).append("' && ENABLED == true");
        }
        ASTJexlScript tree = JexlASTHelper.parseAndFlattenJexlQuery(query.toString());
        assertEquals(TERMS, TermCountingVisitor.countTerms(tree));
        assertTrue(DepthVisitor.getDepth(tree, 10) < 10);

        JexlScript script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query.toString());
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set("FIELD", Integer.toString(TERMS / 2 - 1));
        context.set("ENABLED", true);
        assertTrue(ArithmeticJexlEngines.isMatched(script.execute(context)));
        context.set("ENABLED", false);
        assertFalse(ArithmeticJexlEngines.isMatched(script.execute(context)));
    }

    @Test
    void parsesLargeQueriesConcurrentlyWithoutSharingEvaluationState() throws Exception {
        DatawaveJexlEngine engine = ArithmeticJexlEngines.getEngine(new DefaultArithmetic());
        String disjunction = query(" || ");
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                String request = Integer.toString(i);
                results.add(executor.submit(() -> {
                    start.await();
                    JexlScript script = engine.createScript("(" + disjunction + ") && REQUEST == '" + request + "'");
                    DatawaveJexlContext context = new DatawaveJexlContext();
                    context.set("FIELD", Integer.toString(TERMS - 1));
                    context.set("REQUEST", request);
                    boolean matched = ArithmeticJexlEngines.isMatched(script.execute(context));
                    context.set("REQUEST", "different");
                    return matched && !ArithmeticJexlEngines.isMatched(script.execute(context));
                }));
            }
            start.countDown();
            for (Future<Boolean> result : results) {
                assertTrue(result.get(60, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private void assertWideJunction(ASTJexlScript script, String operator) {
        JexlNode junction = script.jjtGetChild(0);
        if (operator.equals(" || ")) {
            assertInstanceOf(ASTOrNode.class, junction);
        } else {
            assertInstanceOf(ASTAndNode.class, junction);
        }
        assertEquals(TERMS, junction.jjtGetNumChildren());
        assertTrue(DepthVisitor.getDepth(script, 10) < 10);
    }

    private String query(String operator) {
        String comparison = operator.equals(" || ") ? " == '" : " != '";
        StringBuilder query = new StringBuilder();
        for (int i = 0; i < TERMS; i++) {
            if (i > 0) {
                query.append(operator);
            }
            query.append("FIELD").append(comparison).append(i).append("'");
        }
        return query.toString();
    }
}
