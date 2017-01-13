package nsa.datawave.query.language.tree;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nsa.datawave.query.language.functions.lucene.Exclude;
import nsa.datawave.query.language.functions.lucene.Include;
import nsa.datawave.query.language.functions.lucene.IsNotNull;
import nsa.datawave.query.language.functions.lucene.IsNull;
import nsa.datawave.query.language.functions.lucene.LuceneQueryFunction;
import nsa.datawave.query.language.functions.lucene.Occurrence;
import nsa.datawave.query.search.FieldedTerm;
import nsa.datawave.query.search.Term;

import org.apache.log4j.Logger;

public class FunctionNode extends QueryNode {
    private final static Logger log = Logger.getLogger(FunctionNode.class.getName());
    
    private Term query;
    private LuceneQueryFunction function = null;
    
    public FunctionNode(LuceneQueryFunction function, List<String> parameterList, int depth, org.apache.lucene.queryparser.flexible.core.nodes.QueryNode parent) {
        
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
