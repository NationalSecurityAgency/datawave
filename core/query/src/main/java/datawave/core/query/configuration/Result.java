package datawave.core.query.configuration;

import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public class Result<T extends ResultContext> implements Map.Entry<Key,Value> {
    private final T context;
    private final Key key;
    private Value value;

    public Result(Key k, Value v) {
        this(null, k, v);
    }

    public Result(T context, Key k, Value v) {
        this.context = context;
        this.key = k;
        this.value = v;
    }

    public T getContext() {
        return context;
    }

    @Override
    public Key getKey() {
        return key;
    }

    @Override
    public Value getValue() {
        return value;
    }

    @Override
    public Value setValue(Value value) {
        throw new UnsupportedOperationException("This value is immutable");
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Result) {
            Result<?> other = (Result<?>) o;
            return new EqualsBuilder().append(context, other.context).append(key, other.key).append(value, other.value).isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(context).append(key).append(value).toHashCode();
    }

    public Map.Entry<Key,Value> returnKeyValue() {
        Map.Entry<Key,Value> entry = (key == null ? null : Maps.immutableEntry(key, value));
        if (context != null) {
            context.setLastResult(entry == null ? null : entry.getKey());
        }
        return entry;
    }

    public static Iterator<Map.Entry<Key,Value>> keyValueIterator(Iterator<Result> it) {
        return Iterators.filter(Iterators.transform(it, new Function<Result,Map.Entry<Key,Value>>() {
            @Override
            public Map.Entry<Key,Value> apply(@Nullable Result input) {
                if (input == null) {
                    return null;
                }
                return input.returnKeyValue();
            }
        }), new Predicate<Map.Entry<Key,Value>>() {
            @Override
            public boolean apply(@Nullable Map.Entry<Key,Value> keyValueEntry) {
                return keyValueEntry != null;
            }
        });
    }

    public static Iterator<Result> resultIterator(final ResultContext context, Iterator<Map.Entry<Key,Value>> it) {
        return Iterators.filter(Iterators.transform(it, new Function<Map.Entry<Key,Value>,Result>() {
            @Nullable
            @Override
            public Result apply(@Nullable Map.Entry<Key,Value> keyValueEntry) {
                if (keyValueEntry == null) {
                    return null;
                }
                return new Result(context, keyValueEntry.getKey(), keyValueEntry.getValue());
            }
        }), new Predicate<Result>() {

            @Override
            public boolean apply(@Nullable Result result) {
                return result != null;
            }
        });
    }
}
