package datawave.test.iter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A sorted key value iterator that will a {@link RuntimeException} or a subclass thereof.
 */
public class IOExceptionIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    public static String EXCEPTION_CLASS = "exception.class";
    public static String EXCEPTION_MESSAGE = "exception.message";
    public static String FIRE_ON_SEEK = "fireOnSeek";
    public static String FIRE_ON_NEXT = "fireOnNext";
    public static String FIRE_RANDOMLY = "fireRandomly";

    private Key tk;
    private Value tv;

    private SortedKeyValueIterator<Key,Value> source;

    private Class<? extends IOException> exceptionClazz;
    private String exceptionMsg;

    private boolean fireOnSeek = false;
    private boolean fireOnNext = false;
    private boolean fireRandomly = false;

    private final Random random = new Random();

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        validateOptions(options);
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = new IteratorOptions(getClass().getName(), "Throws an exception", null, null);
        options.addNamedOption(EXCEPTION_CLASS, "The class of the thrown exception");
        options.addNamedOption(EXCEPTION_MESSAGE, "The message of the thrown exception");
        options.addNamedOption(FIRE_ON_SEEK, "Fire the exception on the first seek");
        options.addNamedOption(FIRE_ON_NEXT, "Fire the exception on the first next");
        options.addNamedOption(FIRE_RANDOMLY, "Fire the exception randomly (10% chance)");
        return options;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean validateOptions(Map<String,String> map) {
        String option = map.get(EXCEPTION_CLASS);
        if (option == null) {
            throw new IllegalStateException("Missing option: " + EXCEPTION_CLASS);
        } else {
            try {

                exceptionClazz = (Class<? extends IOException>) Class.forName(option);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Class not found: " + EXCEPTION_CLASS);
            }
        }

        option = map.get(EXCEPTION_MESSAGE);
        if (option == null) {
            throw new IllegalStateException("Missing option: " + EXCEPTION_MESSAGE);
        } else {
            exceptionMsg = option;
        }

        option = map.get(FIRE_ON_SEEK);
        if (option != null) {
            fireOnSeek = Boolean.parseBoolean(option);
        }

        option = map.get(FIRE_ON_NEXT);
        if (option != null) {
            fireOnNext = Boolean.parseBoolean(option);
        }

        option = map.get(FIRE_RANDOMLY);
        if (option != null) {
            fireRandomly = Boolean.parseBoolean(option);
        }

        if (!(fireOnSeek || fireOnNext || fireRandomly)) {
            throw new IllegalStateException("At least one fire option must be specified");
        }

        return true;
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        if (fireOnNext) {
            fireException();
        }
        tk = null;
        tv = null;
        if (source.hasTop()) {
            tk = source.getTopKey();
            tv = source.getTopValue();
            source.next();
        }
        if (fireRandomly && random.nextInt(10) == 0) {
            fireException();
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (fireOnSeek) {
            fireException();
        }
        source.seek(range, columnFamilies, inclusive);
        if (fireRandomly && random.nextInt(10) == 0) {
            fireException();
        }
        next();
    }

    private void fireException() throws IOException {
        try {
            Constructor<? extends IOException> constructor = exceptionClazz.getConstructor(String.class);
            throw constructor.newInstance(exceptionMsg);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        IOExceptionIterator copy = new IOExceptionIterator();
        copy.source = source.deepCopy(env);
        return copy;
    }

}
