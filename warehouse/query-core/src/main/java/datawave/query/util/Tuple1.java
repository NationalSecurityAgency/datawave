package datawave.query.util;

public class Tuple1<A> {
    private A first;

    public Tuple1(A a) {
        first = a;
    }

    public A first() {
        return first;
    }

    @Override
    public String toString() {
        return "[" + first() + "]";
    }
}
