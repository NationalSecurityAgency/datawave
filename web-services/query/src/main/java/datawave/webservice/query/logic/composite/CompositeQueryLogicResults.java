package datawave.webservice.query.logic.composite;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class CompositeQueryLogicResults implements Iterable<Object> {
    
    private ArrayBlockingQueue<Object> results;
    private CountDownLatch completionLatch;
    
    public CompositeQueryLogicResults(int pagesize, CountDownLatch completionLatch) {
        this.results = new ArrayBlockingQueue<>(pagesize);
        this.completionLatch = completionLatch;
    }
    
    public void add(Object object) throws InterruptedException {
        this.results.put(object);
    }
    
    public void clear() {
        this.results.clear();
    }
    
    public int size() {
        return results.size();
    }
    
    public boolean contains(Object o) {
        return results.contains(o);
    }
    
    @Override
    public Iterator<Object> iterator() {
        return new CompositeQueryLogicResultsIterator(this.results, this.completionLatch);
    }
    
}
