package datawave.query.rewrite.function;

public interface ConfiguredFunction<A,B> extends com.google.common.base.Function<A,B> {
    public void configure(java.util.Map<String,String> options);
}
