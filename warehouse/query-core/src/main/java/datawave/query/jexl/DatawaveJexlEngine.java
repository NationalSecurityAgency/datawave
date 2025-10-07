package datawave.query.jexl;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.JexlInfo;
import org.apache.commons.jexl3.JexlOptions;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.internal.Engine;
import org.apache.commons.jexl3.internal.Frame;
import org.apache.commons.jexl3.internal.Interpreter;
import org.apache.commons.jexl3.internal.Script;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.jexl3.parser.ASTJexlScript;

/**
 * An extension of the {@link JexlEngine} with reasonable default configurations for DataWave's use-case.
 * <p>
 * This class will pass its own {@link JexlFeatures} and {@link JexlInfo} for performance reasons.
 * <p>
 * Use the JexlFeatures provided by the JexlASTHelper. Many optional flags are disabled for performance reasons.
 * <p>
 * The default constructor for {@link JexlInfo} is decidedly pessimistic. It will create a stack trace with the assumption that an error will be thrown. This
 * can be avoided by using an alternate constructor.
 */
public class DatawaveJexlEngine extends Engine {

    /**
     * Default constructor that should not be used
     */
    @Deprecated
    public DatawaveJexlEngine() {
        super(new JexlBuilder().debug(false).namespaces(ArithmeticJexlEngines.functions()).permissions(JexlPermissions.UNRESTRICTED));
    }

    /**
     * Constructor that accepts a {@link JexlBuilder}
     *
     * @param jexlBuilder
     *            a {@link JexlBuilder}
     */
    public DatawaveJexlEngine(JexlBuilder jexlBuilder) {
        super(jexlBuilder);
    }

    @Override
    protected Interpreter createInterpreter(JexlContext context, Frame frame, JexlOptions opts) {
        return new DatawaveInterpreter(this, opts, context, frame);
    }

    /**
     * Overrides {@link Engine#createScript(JexlFeatures, JexlInfo, String, String...)} so we can inject our own instance of {@link JexlFeatures} and
     * {@link JexlInfo}.
     *
     * @param features
     *            A set of features that will be enforced during parsing
     * @param info
     *            An info structure to carry debugging information if needed
     * @param source
     *            A string containing valid JEXL syntax
     * @param names
     *            The script parameter names used during parsing; a corresponding array of arguments containing values should be used during evaluation
     * @return a Jexl internal {@link Script}
     */
    @Override
    public Script createScript(JexlFeatures features, JexlInfo info, String source, String... names) {
        // Passing in JexlFeatures can cut parse time by 50%
        // Passing JexlInfo can cut parse time by 10%
        if (features == null) {
            features = JexlASTHelper.jexlFeatures();
        }
        if (info == null) {
            info = JexlASTHelper.jexlInfo("DatawaveJexlEngine");
        }
        return super.createScript(features, info, source, names);
    }

    /**
     * Create a Script that supports complex functions
     *
     * @param expression
     *            the jexl expression
     * @return a Script
     */
    public Script createComplexScript(String expression) {
        JexlFeatures features = new JexlFeatures();
        JexlInfo info = JexlASTHelper.jexlInfo("DatawaveJexlEngine");
        return super.createScript(features, info, expression, (String[]) null);
    }

    /**
     * Parses a string into a {@link JexlScript}
     *
     * @param expression
     *            a query string
     * @return a JexlScript
     */
    public ASTJexlScript parse(String expression) {
        // Passing JexlInfo can cut parse time by two orders of magnitude
        JexlInfo info = JexlASTHelper.jexlInfo("DatawaveJexlEngine");
        return super.parse(info, true, expression, null);
    }
}
