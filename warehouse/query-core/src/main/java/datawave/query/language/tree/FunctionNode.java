package datawave.query.language.tree;

import java.util.List;

import org.apache.log4j.Logger;

import datawave.query.language.functions.lucene.LuceneQueryFunction;
import datawave.query.search.FieldedTerm;
import datawave.query.search.Term;

public class FunctionNode extends QueryNode {
    private static final Logger log = Logger.getLogger(FunctionNode.class.getName());

    private Term query;
    private LuceneQueryFunction function = null;

    public FunctionNode(LuceneQueryFunction function, List<String> parameterList, int depth,
                    org.apache.lucene.queryparser.flexible.core.nodes.QueryNode parent) {

        this.function = function;
        this.function.initialize(parameterList, depth, parent);
        this.query = this.function.getFieldedTerm();
    }

    public FunctionNode(FieldedTerm fieldedTerm) {
        super(null, null);
        this.query = fieldedTerm;
    }

    @Override
    public String toString() {
        return query.toString().replaceAll("\0", "");
    }

    /**
     * Since this node does not have any children, return toString()
     */
    @Override
    public String getContents() {
        return toString();
    }

    @Override
    protected boolean isParentDifferent() {
        return true;
    }

    @Override
    public QueryNode clone() {
        LuceneQueryFunction f = (LuceneQueryFunction) this.function.duplicate();
        return new FunctionNode(f.getFieldedTerm());
    }

    public void setQuery(FieldedTerm query) {
        this.query = query;
    }

    public Term getQuery() {
        return query;
    }
}
