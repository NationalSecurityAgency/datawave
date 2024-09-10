package datawave.query.util;

import com.google.common.base.Objects;

public class Tuple4<A,B,C,D> extends Tuple3<A,B,C> {
    private D fourth;

    public Tuple4(A a, B b, C c, D d) {
        super(a, b, c);
        fourth = d;
    }

    public D fourth() {
        return fourth;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Tuple4))
            return false;
        if (!super.equals(o))
            return false;
        Tuple4 other = (Tuple4) o;
        return Objects.equal(fourth, other.fourth);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), fourth);
    }

    @Override
    public String toString() {
        return "[" + first() + "," + second() + "," + third() + "," + fourth() + "]";
    }
}
