/**
 *
 */
package datawave.metrics.util;

/**
 *
 * A class that averages integers. It acts a mutable <code>Number</code>, although it is not thread-safe.
 *
 */
public class IntegerAverage extends Number {
    private static final long serialVersionUID = -7988336409833740153L;

    private long count;
    private long sum;

    public IntegerAverage() {
        count = 0;
        sum = 0;
    }

    public IntegerAverage(long l) {
        sum = l;
        count = 1;
    }

    public void add(long i) {
        sum += i;
        ++count;
    }

    public void add(IntegerAverage other) {
        sum += other.sum;
        count += other.count;
    }

    public void add(int i) {
        sum += i;
        ++count;
    }

    public long getCount() {
        return count;
    }

    public long getSum() {
        return sum;
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public long longValue() {
        return count == 0 ? 0 : sum / count;
    }

    @Override
    public float floatValue() {
        return (float) longValue();
    }

    @Override
    public double doubleValue() {
        return (double) longValue();
    }

    @Override
    public String toString() {
        return Double.toString(longValue());
    }
}
