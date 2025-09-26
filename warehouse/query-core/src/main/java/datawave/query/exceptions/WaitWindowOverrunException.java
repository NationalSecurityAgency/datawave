package datawave.query.exceptions;

import java.util.LinkedList;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang3.tuple.Pair;

public class WaitWindowOverrunException extends RuntimeException {

    /*
     * Used to track the history of yieldKeys as the Exception gets propagated up the boolean iterator stack and the yield key gets replaced. The original
     * yieldKey will be first and the most recent yieldKey will be last
     */
    private LinkedList<Pair<Key,String>> yieldKeys = new LinkedList<>();

    public WaitWindowOverrunException(Pair<Key,String> yieldKey) {
        this.yieldKeys.addLast(yieldKey);
    }

    /*
     * not a valid constructor - must have a yield key and description
     */
    private WaitWindowOverrunException() {}

    @Override
    public String getMessage() {
        return getYieldKey().toString();
    }

    /*
     * The most recent yieldKey will be first
     */
    public Pair<Key,String> getYieldKey() {
        return yieldKeys.isEmpty() ? null : yieldKeys.getLast();
    }

    /*
     * The original yieldKey will be first and the most recent yieldKey will be last
     */
    public LinkedList<Pair<Key,String>> getYieldKeyHistory() {
        return yieldKeys;
    }

    /*
     * Only add if yieldKeys is empty or if yieldKey.getFirst() is different than the current top Key
     */
    public void setYieldKey(Pair<Key,String> yieldKey) {
        if (this.yieldKeys.isEmpty()) {
            this.yieldKeys.addLast(yieldKey);
        } else {
            Key newKey = yieldKey.getLeft();
            Key lastKey = this.yieldKeys.getLast().getLeft();
            boolean bothNull = newKey == null && lastKey == null;
            boolean eitherNull = newKey == null || lastKey == null;
            // tread carefully here since we can't dereference a null
            if (!bothNull && (eitherNull || !lastKey.equals(newKey))) {
                this.yieldKeys.addLast(yieldKey);
            }
        }
    }
}
