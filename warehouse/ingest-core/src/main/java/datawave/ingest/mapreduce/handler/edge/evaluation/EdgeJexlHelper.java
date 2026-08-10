package datawave.ingest.mapreduce.handler.edge.evaluation;

import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.JexlInfo;

/**
 * Jexl utility for edge preconditions
 */
public class EdgeJexlHelper {

    private static final String DEFAULT_STAGE = "EdgeJexlEngine";

    private EdgeJexlHelper() {
        // enforce static utility
    }

    public static JexlInfo jexlInfo() {
        return jexlInfo(DEFAULT_STAGE);
    }

    public static JexlInfo jexlInfo(String stage) {
        return new JexlInfo(stage, 1, 1);
    }

    /**
     * Copied from datawave query core until this can be a true core utility
     *
     * @return JexlFeatures
     */
    public static JexlFeatures jexlFeatures() {
        // @formatter:off
        return new JexlFeatures()
                // mostly used internally by Jexl
                .register(false) // default false
                // allow usage of let, const, and var variables
                .localVar(false) // default true
                // allow side-effect operators (e.g. +=, -=, |=, <<=, etc) for global vars.  needed to assign values (e.g. _Value_ = true)
                .sideEffectGlobal(true) // default true
                // allow side-effect operators (e.g. +=, -=, |=, <<=, etc).  needed to assign values (e.g. _Value_ = true)
                .sideEffect(true) // default true
                // allow array indexing via reference expression (e.g. array[some expression])
                .arrayReferenceExpr(false) // default true
                // allow method calls on expressions
                .methodCall(true) // default true
                // allow array/map/set literal expressions (e.g. [1, 2, "three"], {"one": 1, "two": 2}, {"one", 2, "more"}
                .structuredLiteral(false) // default true
                // allow creation of new instances
                .newInstance(false) // default true
                // allow loop constructs
                .loops(false) // default true
                // allow lambda/function constructs (not the same as namespaced functions)
                .lambda(false) // default true
                // allow thin-arrow lambda syntax (e.g. ->)
                .thinArrow(false) // default true
                // allow fat-arrow lambda syntax (e.g. =>)
                .fatArrow(false) // default false
                // allow comparator names (e.g. eq, ne, lt, gt, etc)
                .comparatorNames(false) // default true
                // allow pragma constructs (e.g. #pragma some.option some-value)
                .pragma(false) // default true
                // allow pragma constructs anywhere in the code
                .pragmaAnywhere(false) // default true
                // allow namespace pragmas (e.g. '#pragma jexl.namespace.str java.lang.String' to use 'str' in place of 'java.lang.String')
                .namespacePragma(false) // default true
                // allow import pragmas (e.g. '#pragma jexl.import java.net' to use new URL() instead of new java.net.URL())
                .importPragma(false) // default true
                // allow annotations
                .annotation(false) // default true
                // allow multiple semicolon-terminated expressions per jexl string
                .script(false) // default true
                // whether redefining local variables is an error
                .lexical(false) // default false
                // whether local variables shade global variables
                .lexicalShade(false); // default false
        // @formatter: on
    }
}
