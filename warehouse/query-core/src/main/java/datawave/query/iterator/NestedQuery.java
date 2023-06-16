package datawave.query.iterator;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import datawave.query.function.JexlEvaluation;

public class NestedQuery<T> {
    protected NestedIterator<T> iter;
    protected Range iterRange;
    protected ASTJexlScript script;
    protected String queryString;
    protected JexlEvaluation eval;

    public void setQuery(String query) {
        this.queryString = query;
    }

    public String getQuery() {
        return queryString;
    }

    public void setQueryScript(ASTJexlScript script) {
        this.script = script;
    }

    public ASTJexlScript getScript() {
        return script;
    }

    public void setRange(Range range) {
        this.iterRange = range;
    }

    public Range getRange() {
        return iterRange;
    }

    public void setIterator(NestedIterator<T> iter) {
        this.iter = iter;
    }

    public NestedIterator<T> getIter() {
        return iter;
    }

    public void setEvaluation(JexlEvaluation eval) {
        this.eval = eval;

    }

    public JexlEvaluation getEvaluation() {
        return eval;
    }
}
