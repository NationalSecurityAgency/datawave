package datawave.query.jexl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl2.Interpreter;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.introspection.Uberspect;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.logging.Log;

/**
 * Extension of the JexlEngine.
 *
 */
public class DatawaveJexlEngine extends JexlEngine {

    // support for a partial document evaluation
    private boolean usePartialInterpreter = false;
    private Set<String> incompleteFields = Collections.emptySet();
    private PartialInterpreterCallback callback = new PartialInterpreterCallback();

    public DatawaveJexlEngine() {
        super();
        setDebug(false);
        registerFunctions();
    }

    public DatawaveJexlEngine(Uberspect anUberspect, JexlArithmetic anArithmetic, Map<String,Object> theFunctions, Log log) {
        super(anUberspect, anArithmetic, theFunctions, log);
    }

    private void registerFunctions() {
        this.setFunctions(ArithmeticJexlEngines.functions());
    }

    @Override
    protected Interpreter createInterpreter(JexlContext context, boolean strictFlag, boolean silentFlag) {
        if (usePartialInterpreter) {
            return new DatawavePartialInterpreter(this, context, strictFlag, silentFlag, incompleteFields, callback);
        } else {
            return new DatawaveInterpreter(this, context, strictFlag, silentFlag);
        }
    }

    public ASTJexlScript parse(CharSequence expression) {
        return super.parse(expression, null, null);
    }

    public boolean getUsePartialInterpreter() {
        return this.usePartialInterpreter;
    }

    public void setUsePartialInterpreter(boolean usePartialInterpreter) {
        this.usePartialInterpreter = usePartialInterpreter;
    }

    public Set<String> getIncompleteFields() {
        return this.incompleteFields;
    }

    public void setIncompleteFields(Set<String> incompleteFields) {
        this.incompleteFields = incompleteFields;
    }

    public boolean wasCallbackUsed() {
        return this.callback.getIsUsed();
    }

    public void resetCallback() {
        this.callback.reset();
    }
}
