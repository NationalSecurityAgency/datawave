package org.apache.commons.jexl3.internal;

import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.parser.ASTJexlScript;

import datawave.core.query.jexl.visitors.TreeFlatteningRebuildingVisitor;

/**
 * Lifted from {@link Script}. Modified to flatten the resulting {@link ASTJexlScript}.
 */
public class DatawaveJexlScript extends Script implements JexlExpression {

    /** The engine for this expression. */
    protected Engine jexl;
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
    protected DatawaveJexlScript(Engine engine, String expr, ASTJexlScript ref) {
        super(engine, expr, ref);
        jexl = engine;
        expression = expr;
        script = TreeFlatteningRebuildingVisitor.flatten(ref);
    }

    public static DatawaveJexlScript create(Script expression) {
        return new DatawaveJexlScript(expression.jexl, expression.source, expression.script);
    }

    /**
     * {@inheritDoc}
     */
    public Object evaluate(JexlContext context) {
        if (script.jjtGetNumChildren() < 1) {
            return null;
        }
        Interpreter interpreter = jexl.createInterpreter(context, script.createFrame((Object[]) null), null);
        return interpreter.interpret(script.jjtGetChild(0));
    }

    /**
     * {@inheritDoc}
     */
    public String dump() {
        Debugger debug = new Debugger();
        boolean d = debug.debug(script);
        return debug.data(script) + (d ? " /*" + debug.start() + ":" + debug.end() + "*/" : "/*?:?*/ ");
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
        Interpreter interpreter = jexl.createInterpreter(context, script.createFrame((Object[]) null), null);
        return interpreter.interpret(script);
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.1
     */
    public Object execute(JexlContext context, Object... args) {
        Interpreter interpreter = jexl.createInterpreter(context, script.createFrame(args), null);
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
        return jexl.getVariables(script);
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.1
     */
    public Callable callable(JexlContext context) {
        return callable(context, (Object[]) null);
    }

    /**
     * {@inheritDoc}
     *
     * @since 2.1
     */
    public Callable callable(JexlContext context, Object... args) {
        final Interpreter interpreter = jexl.createInterpreter(context, script.createFrame(args), null);

        return new Callable(interpreter) {
            public Object call() throws Exception {
                if (result == interpreter) {
                    result = interpreter.interpret(script);
                }
                return result;
            }

        };
    }

}
