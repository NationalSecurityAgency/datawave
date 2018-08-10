package datawave.webservice.query.iterator;

import datawave.webservice.query.logic.FlushableQueryLogicTransformer;
import java.util.Iterator;

import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.TransformIterator;
import datawave.webservice.query.exception.EmptyObjectException;
import org.apache.log4j.Logger;

public class DatawaveTransformIterator extends TransformIterator {
    
    private Logger log = Logger.getLogger(DatawaveTransformIterator.class);
    private Object next = null;
    
    public DatawaveTransformIterator() {
        super();
    }
    
    public DatawaveTransformIterator(Iterator iterator) {
        super(iterator);
    }
    
    public DatawaveTransformIterator(Iterator iterator, Transformer transformer) {
        super(iterator, transformer);
    }
    
    @Override
    public boolean hasNext() {
        
        if (next == null) {
            next = getNext();
        }
        return (next != null);
    }
    
    @Override
    public Object next() {
        
        Object o = null;
        if (next == null) {
            o = getNext();
        } else {
            o = next;
            next = null;
        }
        return o;
    }
    
    private Object getNext() {
        
        boolean done = false;
        Object o = null;
        while (super.hasNext() && !done) {
            try {
                o = super.next();
                done = true;
            } catch (EmptyObjectException e) {
                // not yet done, so continue fetching next
            }
        }
        // see if there are any results cached by the transformer
        if (o == null && getTransformer() instanceof FlushableQueryLogicTransformer) {
            done = false;
            while (!done) {
                try {
                    o = ((FlushableQueryLogicTransformer) getTransformer()).flush();
                    done = true;
                } catch (EmptyObjectException e) {
                    // not yet done, so continue flushing
                }
            }
        }
        return o;
    }
}
