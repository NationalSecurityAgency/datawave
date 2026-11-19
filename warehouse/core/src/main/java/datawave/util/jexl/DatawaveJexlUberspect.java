package datawave.util.jexl;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlCache;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlOperator;
import org.apache.commons.jexl3.internal.Engine;
import org.apache.commons.jexl3.introspection.JexlMethod;
import org.apache.commons.jexl3.introspection.JexlPropertyGet;
import org.apache.commons.jexl3.introspection.JexlPropertySet;
import org.apache.commons.jexl3.introspection.JexlUberspect;

/**
 * Reuses JEXL 3.5's operator dispatcher for an engine's configured arithmetic. The dispatcher contains no evaluation context; arithmetic variants created for
 * individual requests still receive their own dispatcher. All other introspection uses the engine's usual shared uberspect.
 */
public final class DatawaveJexlUberspect implements JexlUberspect {
    private final JexlUberspect delegate;
    private final JexlArithmetic arithmetic;
    private volatile JexlOperator.Uberspect operators;

    private DatawaveJexlUberspect(JexlUberspect delegate, JexlArithmetic arithmetic) {
        this.delegate = delegate;
        this.arithmetic = arithmetic;
    }

    public static JexlBuilder configure(JexlBuilder builder) {
        // A builder may be reused with different arithmetic, permissions, or cache settings. Keep dispatchers engine-local.
        if (builder.uberspect() instanceof DatawaveJexlUberspect) {
            builder.uberspect(null);
        }
        if (builder.uberspect() == null && builder.arithmetic() != null) {
            JexlUberspect delegate = Engine.getUberspect(builder.logger(), builder.strategy(), builder.permissions());
            builder.uberspect(new DatawaveJexlUberspect(delegate, builder.arithmetic()));
        }
        return builder;
    }

    @Override
    public JexlOperator.Uberspect getOperator(JexlArithmetic requestArithmetic) {
        if (requestArithmetic != arithmetic) {
            return delegate.getOperator(requestArithmetic);
        }
        JexlOperator.Uberspect result = operators;
        if (result == null) {
            synchronized (this) {
                result = operators;
                if (result == null) {
                    operators = result = new DatawaveOperator(delegate.getOperator(arithmetic), arithmetic);
                }
            }
        }
        return result;
    }

    @Override
    public synchronized void setClassLoader(ClassLoader loader) {
        delegate.setClassLoader(loader);
        operators = null;
    }

    @Override
    public JexlArithmetic.Uberspect getArithmetic(JexlArithmetic requestArithmetic) {
        return delegate.getArithmetic(requestArithmetic);
    }

    @Override
    public Class<?> getClassByName(String className) {
        return delegate.getClassByName(className);
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getClassLoader();
    }

    @Override
    public int getVersion() {
        return delegate.getVersion();
    }

    @Override
    public JexlMethod getConstructor(Object constructor, Object... args) {
        return delegate.getConstructor(constructor, args);
    }

    @Override
    public Iterator<?> getIterator(Object object) {
        return delegate.getIterator(object);
    }

    @Override
    public JexlMethod getMethod(Object object, String method, Object... args) {
        return delegate.getMethod(object, method, args);
    }

    @Override
    public JexlPropertyGet getPropertyGet(List<PropertyResolver> resolvers, Object object, Object identifier) {
        return delegate.getPropertyGet(resolvers, object, identifier);
    }

    @Override
    public JexlPropertyGet getPropertyGet(Object object, Object identifier) {
        return delegate.getPropertyGet(object, identifier);
    }

    @Override
    public JexlPropertySet getPropertySet(List<PropertyResolver> resolvers, Object object, Object identifier, Object value) {
        return delegate.getPropertySet(resolvers, object, identifier, value);
    }

    @Override
    public JexlPropertySet getPropertySet(Object object, Object identifier, Object value) {
        return delegate.getPropertySet(object, identifier, value);
    }

    @Override
    public List<PropertyResolver> getResolvers(JexlOperator operator, Object object) {
        return delegate.getResolvers(operator, object);
    }

    private static final class DatawaveOperator implements JexlOperator.Uberspect {
        private final JexlOperator.Uberspect delegate;
        private final JexlArithmetic arithmetic;

        private DatawaveOperator(JexlOperator.Uberspect delegate, JexlArithmetic arithmetic) {
            this.delegate = delegate;
            this.arithmetic = arithmetic;
        }

        @Override
        public Object tryOverload(JexlCache.Reference node, JexlOperator operator, Object... args) {
            if (!overloads(operator) && !overloads(JexlOperator.COMPARE)) {
                // Preserve JEXL 3.5's null checks even when there is no overload to resolve.
                for (Object arg : args) {
                    if (arg == null) {
                        if (arithmetic.isStrict(operator)) {
                            throw new JexlArithmetic.NullOperand();
                        }
                        break;
                    }
                }
                return JexlEngine.TRY_FAILED;
            }
            return delegate.tryOverload(node, operator, args);
        }

        @Override
        public boolean overloads(JexlOperator operator) {
            return delegate.overloads(operator);
        }

        @Override
        public JexlMethod getOperator(JexlOperator operator, Object... args) {
            return delegate.getOperator(operator, args);
        }

        @Override
        public Object tryAssignOverload(JexlCache.Reference node, JexlOperator operator, Consumer<Object> assign, Object... args) {
            return delegate.tryAssignOverload(node, operator, assign, args);
        }

        @Override
        public Object size(JexlCache.Reference node, Object object) {
            return delegate.size(node, object);
        }

        @Override
        public Object empty(JexlCache.Reference node, Object object) {
            return delegate.empty(node, object);
        }

        @Override
        public boolean contains(JexlCache.Reference node, JexlOperator operator, Object left, Object right) {
            return delegate.contains(node, operator, left, right);
        }

        @Override
        public boolean startsWith(JexlCache.Reference node, JexlOperator operator, Object left, Object right) {
            return delegate.startsWith(node, operator, left, right);
        }

        @Override
        public boolean endsWith(JexlCache.Reference node, JexlOperator operator, Object left, Object right) {
            return delegate.endsWith(node, operator, left, right);
        }
    }
}
