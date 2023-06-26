package datawave.core.query.iterator;

import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.Flushable;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import java.util.Iterator;

public class DatawaveTransformIterator<I,O> extends TransformIterator<I,O> {

    private Logger log = Logger.getLogger(DatawaveTransformIterator.class);
    private O next = null;

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
    public O next() {

        O o = null;
        if (next == null) {
            o = getNext();
        } else {
            o = next;
            next = null;
        }
        return o;
    }

    private O getNext() {

        boolean done = false;
        O o = null;
        while (super.hasNext() && !done) {
            try {
                o = super.next();
                done = true;
            } catch (EmptyObjectException e) {
                // not yet done, so continue fetching next
            }
        }
        // see if there are any results cached by the transformer
        if (o == null && getTransformer() instanceof Flushable) {
            done = false;
            while (!done) {
                try {
                    o = ((Flushable<O>) getTransformer()).flush();
                    done = true;
                } catch (EmptyObjectException e) {
                    // not yet done, so continue flushing
                }
            }
        }
        return o;
    }
}
