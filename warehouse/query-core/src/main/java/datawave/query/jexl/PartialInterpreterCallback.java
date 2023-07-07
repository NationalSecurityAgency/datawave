package datawave.query.jexl;

/**
 * A callback set on the {@link DatawavePartialInterpreter} that informs the {@link datawave.query.function.JexlEvaluation} if a partial evaluation occurred
 */
public class PartialInterpreterCallback {

    private boolean used = false;

    public boolean getIsUsed() {
        return used;
    }

    public void setUsed(boolean used) {
        this.used = used;
    }

    public void reset() {
        this.used = false;
    }
}
