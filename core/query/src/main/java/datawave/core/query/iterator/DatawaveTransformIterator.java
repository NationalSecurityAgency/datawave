package datawave.core.query.iterator;

import java.util.Iterator;

import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.Flushable;

public class DatawaveTransformIterator<I,O> extends TransformIterator<I,O> {

    private Logger log = LoggerFactory.getLogger(DatawaveTransformIterator.class);
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
