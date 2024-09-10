package datawave.query.planner.pushdown;

/**
 *
 */
public class Cost implements Comparable<Cost> {
    private long erCost = 0l, otherCost = 0l;

    protected boolean evaluated = false;

    public static final Cost UNEVALUATED = new Cost();

    public static final Cost INFINITE = new Cost(Long.MAX_VALUE, Long.MIN_VALUE);

    public static final long ER_COST_MULTIPLIER = 2;

    public Cost() {}

    public Cost(long erCost, long otherCost) {
        setERCost(erCost);
        setOtherCost(otherCost);
        evaluated = true;
    }

    public boolean isUnevaluated() {
        return !evaluated;
    }

    public void incrementBy(Cost otherCost) {
        incrementERCost(otherCost.getERCost());
        incrementOtherCost(otherCost.getOtherCost());
    }

    public void incrementERCost(long newCost) {
        long newERCost = erCost + newCost;

        // Quick check for Long overflow
        if (erCost > 0 && newCost > 0 && newERCost < erCost) {
            erCost = Long.MAX_VALUE;
        } else {
            erCost = newERCost;
        }
    }

    public void setERCost(long newCost) {
        erCost = newCost;
    }

    public long getERCost() {
        return erCost;
    }

    public long totalCost() {
        return erCost + otherCost;
    }

    public void incrementOtherCost(long newCost) {
        long newOtherCost = otherCost + newCost;

        if (otherCost > 0 && newCost > 0 && newOtherCost < otherCost) {
            otherCost = Long.MAX_VALUE;
        } else {
            otherCost = newOtherCost;
        }
    }

    public void setOtherCost(long newCost) {
        otherCost = newCost;
    }

    public long getOtherCost() {
        return otherCost;
    }

    @Override
    public String toString() {
        return "Regex cost: " + getERCost() + ", Other cost: " + getOtherCost();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Cost) {
            Cost other = (Cost) o;
            return getERCost() == other.getERCost() && getOtherCost() == other.getOtherCost();
        }

        return false;
    }

    @Override
    public int hashCode() {
        int result = (int) (erCost ^ (erCost >>> 32));
        result = 31 * result + (int) (otherCost ^ (otherCost >>> 32));
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Cost otherCostObj) {
        Long me = Long.valueOf(getERCost() + getOtherCost());
        Long other = Long.valueOf(otherCostObj.getERCost() + otherCostObj.getOtherCost());
        return me.compareTo(other);
    }
}
