package org.apache.commons.jexl2;

import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Lifted from {@link ExpressionImpl}. Modified to flatten the resulting {@link ASTJexlScript}.
 */
public class DatawaveJexlScript implements Expression, Script {

    /** The engine for this expression. */
    protected JexlEngine jexl;
    /**
     * Original expression stripped from leading &amp; trailing spaces.
     */
    protected String expression;
    /**
     * The resulting AST we can interpret.
     */
    protected ASTJexlScript script;

    /**
     * Do not let this be generally instantiated with a 'new'.
     *
     * @param engine
     *            the interpreter to evaluate the expression
     * @param expr
     *            the expression.
     * @param ref
     *            the parsed expression.
     */
    protected DatawaveJexlScript(JexlEngine engine, String expr, ASTJexlScript ref) {
        jexl = engine;
        expression = expr;
        script = TreeFlatteningRebuildingVisitor.flatten(ref);
    }

    public static DatawaveJexlScript create(ExpressionImpl expression) {
        return new DatawaveJexlScript(expression.jexl, expression.expression, expression.script);
    }

    /**
     * {@inheritDoc}
     */
    public Object evaluate(JexlContext context) {
        if (script.jjtGetNumChildren() < 1) {
            return null;
        }
        Interpreter interpreter = jexl.createInterpreter(context);
        interpreter.setFrame(script.createFrame((Object[]) null));
        return interpreter.interpret(script.jjtGetChild(0));
    }

    /**
     * {@inheritDoc}
     */
    public String dump() {
        Debugger debug = new Debugger();
        boolean d = debug.debug(script);
        return debug.data() + (d ? " /*" + debug.start() + ":" + debug.end() + "*/" : "/*?:?*/ ");
    }

    /**
     * {@inheritDoc}
     */
    public String getExpression() {
        return expression;
    }

    /**
     * Provide a string representation of this expression.
     *
     * @return the expression or blank if it's null.
     */
    @Override
    public String toString() {
        String expr = getExpression();
        return expr == null ? "" : expr;
    }

    /**
     * {@inheritDoc}
     */
    public String getText() {
        return toString();
    }

    /**
     * {@inheritDoc}
     */
    public Object execute(JexlContext context) {
        Interpreter interpreter = jexl.createInterpreter(context);
        interpreter.setFrame(script.createFrame((Object[]) null));
        return interpreter.interpret(script);
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.1
     */
    public Object execute(JexlContext context, Object... args) {
        Interpreter interpreter = jexl.createInterpreter(context);
        interpreter.setFrame(script.createFrame(args));
        return interpreter.interpret(script);
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.1
     */
    public String[] getParameters() {
        return script.getParameters();
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.1
     */
    public String[] getLocalVariables() {
        return script.getLocalVariables();
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.1
     */
    public Set<List<String>> getVariables() {
        return jexl.getVariables(this);
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.1
     */
    public Callable<Object> callable(JexlContext context) {
        return callable(context, (Object[]) null);
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.1
     */
    public Callable<Object> callable(JexlContext context, Object... args) {
        final Interpreter interpreter = jexl.createInterpreter(context);
        interpreter.setFrame(script.createFrame(args));

        return new Callable<Object>() {
            /** Use interpreter as marker for not having run. */
            private Object result = interpreter;

            public Object call() throws Exception {
                if (result == interpreter) {
                    result = interpreter.interpret(script);
                }
                return result;
            }

        };
    }

}
