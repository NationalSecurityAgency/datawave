package datawave.query.predicate;

public interface ConfiguredPredicate<A> extends com.google.common.base.Predicate<A> {
    public void configure(java.util.Map<String,String> options);
}
