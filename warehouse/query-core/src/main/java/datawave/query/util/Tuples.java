package datawave.query.util;

public class Tuples {
    public static <A> Tuple1<A> tuple(A a) {
        return new Tuple1<>(a);
    }

    public static <A,B> Tuple2<A,B> tuple(A a, B b) {
        return new Tuple2<>(a, b);
    }

    public static <A,B,C> Tuple3<A,B,C> tuple(A a, B b, C c) {
        return new Tuple3<>(a, b, c);
    }

    public static <A,B,C,D> Tuple4<A,B,C,D> tuple(A a, B b, C c, D d) {
        return new Tuple4<>(a, b, c, d);
    }
}
