package datawave.query.util;

import com.google.common.base.Objects;

public class Tuple3<A,B,C> extends Tuple2<A,B> {
    protected C third;

    public Tuple3(A first, B second, C third) {
        super(first, second);
        this.third = third;
    }

    public C third() {
        return third;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Tuple3))
            return false;
        if (!super.equals(o))
            return false;
        Tuple3 other = (Tuple3) o;
        return Objects.equal(third, other.third);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), third);
    }

    @Override
    public String toString() {
        return "[" + first() + "," + second() + "," + third() + "]";
    }
}
