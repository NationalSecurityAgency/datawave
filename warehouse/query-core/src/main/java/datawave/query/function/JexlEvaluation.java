package datawave.query.function;

import datawave.query.attributes.Attributes;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.DelayedNonEventIndexContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.jexl.DatawaveJexlEngine;
import datawave.query.util.Tuple3;

public class JexlEvaluation implements Predicate<Tuple3<Key,Document,DatawaveJexlContext>> {
    private static final Logger log = Logger.getLogger(JexlEvaluation.class);
    
    public static final String HIT_TERM_FIELD = "HIT_TERM";
    
    private String query;
    private JexlArithmetic arithmetic;
    private DatawaveJexlEngine engine;
    
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
        this.script = this.engine.createScript(this.query);
    }
    
    public JexlArithmetic getArithmetic() {
        return arithmetic;
    }
    
    public DatawaveJexlEngine getEngine() {
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
        
        // Add delayed info to document
        if (matched && input.third() instanceof DelayedNonEventIndexContext) {
            ((DelayedNonEventIndexContext) input.third()).populateDocument(input.second());
        }
        
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
                    document.put(HIT_TERM_FIELD, attributes);
                }
            }
            hitListArithmetic.clear();
        }
        return matched;
    }
    
}
