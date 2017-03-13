package nsa.datawave.query.rewrite.function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

import nsa.datawave.query.jexl.DatawaveJexlContext;
import nsa.datawave.query.rewrite.attributes.Attributes;
import nsa.datawave.query.rewrite.attributes.Content;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.jexl.ArithmeticJexlEngines;
import nsa.datawave.query.rewrite.jexl.DefaultArithmetic;
import nsa.datawave.query.rewrite.jexl.HitListArithmetic;
import nsa.datawave.query.rewrite.jexl.RefactoredDatawaveJexlEngine;
import nsa.datawave.query.util.Tuple3;

public class JexlEvaluation implements Predicate<Tuple3<Key,Document,DatawaveJexlContext>> {
    private static final Logger log = Logger.getLogger(JexlEvaluation.class);
    
    private String query;
    private JexlArithmetic arithmetic;
    private RefactoredDatawaveJexlEngine engine;
    
    /**
     * Compiled jexl script
     */
    protected Script script;
    
    public JexlEvaluation(String query) {
        this(query, new DefaultArithmetic());
    }
    
    public JexlEvaluation(String query, JexlArithmetic arithmetic) {
        this.query = query;
        this.arithmetic = arithmetic;
        
        // Get a JexlEngine initialized with the correct JexlArithmetic for this Document
        this.engine = ArithmeticJexlEngines.getEngine(arithmetic);
        
        // Evaluate the JexlContext against the Script
        this.script = this.engine.createScript(query);
    }
    
    public JexlArithmetic getArithmetic() {
        return arithmetic;
    }
    
    public RefactoredDatawaveJexlEngine getEngine() {
        return engine;
    }
    
    public ASTJexlScript parse(CharSequence expression) {
        return engine.parse(expression);
    }
    
    public boolean isMatched(Object o) {
        return ArithmeticJexlEngines.isMatched(o);
    }
    
    @Override
    public boolean apply(Tuple3<Key,Document,DatawaveJexlContext> input) {
        
        Object o = script.execute(input.third());
        
        if (log.isTraceEnabled()) {
            log.trace("Evaluation of " + query + " against " + input.third() + " returned " + o);
        }
        
        boolean matched = isMatched(o);
        
        if (arithmetic instanceof HitListArithmetic) {
            HitListArithmetic hitListArithmetic = (HitListArithmetic) arithmetic;
            if (matched) {
                Document document = input.second();
                
                Attributes attributes = new Attributes(input.second().isToKeep());
                
                for (String term : hitListArithmetic.getHitSet()) {
                    // get the visibility for the record with this hit
                    ColumnVisibility columnVisibility = HitListArithmetic.getColumnVisibilityForHit(document, term);
                    // if no visibility computed, then there were no hits that match fields still in the document......
                    if (columnVisibility != null) {
                        // unused
                        long timestamp = document.getTimestamp(); // will force an update to make the metadata valid
                        Content content = new Content(term, document.getMetadata(), document.isToKeep());
                        content.setColumnVisibility(columnVisibility);
                        attributes.add(content);
                        
                    }
                }
                if (attributes.size() > 0) {
                    document.put("HIT_TERM", attributes);
                }
            }
            hitListArithmetic.clear();
        }
        
        return matched;
        
    }
    
}
