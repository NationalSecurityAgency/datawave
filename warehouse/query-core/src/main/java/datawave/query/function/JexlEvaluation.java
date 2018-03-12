package datawave.query.function;

import datawave.data.type.BaseType;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.DefaultArithmetic;
import datawave.webservice.results.cached.result.CachedresultMessages;
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

public class JexlEvaluation implements Predicate<Tuple3<Key,Document,DatawaveJexlContext>> {
    private static final Logger log = Logger.getLogger(JexlEvaluation.class);
    
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
        this.script = this.engine.createScript(query);
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
        Document document = input.second();
        
        if (log.isTraceEnabled()) {
            log.trace("Evaluation of " + query + " against " + input.third() + " returned " + o);
        }
        
        boolean matched = isMatched(o);
        
        // If content functions have set a term position and term timestamps exist. Filter the term timestamps to matched positions
        if (input.third().has(Constants.CONTENT_TERM_POSITION_KEY) && document.containsKey(Constants.CONTENT_TERM_TIMESTAMP_KEY)) {
            
            Object oPositions = input.third().get(Constants.CONTENT_TERM_POSITION_KEY);
            if (null != oPositions && oPositions instanceof Collection) {
                // Form map of position to timestamps from document
                Attribute<?> termTS = document.get(Constants.CONTENT_TERM_TIMESTAMP_KEY);
                HashMap<Integer,String> termTSMap = new HashMap<>();
                for (Object ts : (Set<?>) termTS.getData()) {
                    if (ts instanceof Attribute) {
                        Attribute ats = (Attribute) ts;
                        Object data = ats.getData();
                        
                        if (data instanceof BaseType) {
                            String termTime = data.toString();
                            String[] parts = termTime.split("_");
                            termTSMap.put(Integer.parseInt(parts[0]), parts[1]);
                        }
                    }
                }
                
                if (null != termTSMap && termTSMap.size() > 0) {
                    TreeSet<Integer> positions = new TreeSet<>((Collection) oPositions);
                    
                    // Clean all term timestamps and filter to only matched positions, remove position for display
                    document.removeAll(Constants.CONTENT_TERM_TIMESTAMP_KEY);
                    for (Integer position : positions) {
                        int modPosition = position - (position % 10);
                        if (termTSMap.containsKey(modPosition)) {
                            Content content = new Content(termTSMap.get(modPosition), document.getMetadata(), document.isToKeep());
                            document.put(Constants.CONTENT_TERM_TIMESTAMP_KEY, content);
                        }
                    }
                }
            }
        }
        
        if (arithmetic instanceof HitListArithmetic) {
            HitListArithmetic hitListArithmetic = (HitListArithmetic) arithmetic;
            if (matched) {
                
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
