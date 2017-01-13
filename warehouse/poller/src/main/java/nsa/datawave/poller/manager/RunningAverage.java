package nsa.datawave.poller.manager;

import java.util.LinkedList;

public class RunningAverage {
    protected LinkedList<Double> values = new LinkedList<>();
    protected double avg = 0.0d;
    protected int numValues = 100;
    
    public RunningAverage() {}
    
    public RunningAverage(int numValues) {
        this.numValues = numValues;
    }
    
    public boolean isEmpty() {
        return values.isEmpty();
    }
    
    public int size() {
        return values.size();
    }
    
    public void add(Double d) {
        if (values.size() == numValues) {
            double a = values.removeFirst();
            values.addLast(d);
            avg = avg + ((d - a) / values.size());
        } else {
            values.addLast(d);
            avg = avg * (((double) (values.size()) - 1.0d) / (double) (values.size())) + (d / (double) (values.size()));
        }
    }
    
    public double getAverage() {
        return avg;
    }
}
