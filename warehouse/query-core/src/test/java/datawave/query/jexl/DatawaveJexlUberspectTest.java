package datawave.query.jexl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlInfo;
import org.apache.commons.jexl3.JexlOperator;
import org.apache.commons.jexl3.JexlOptions;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.internal.Engine;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.jexl3.introspection.JexlUberspect;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class DatawaveJexlUberspectTest {

    @Test
    void retainsOneDispatcherForTheEngineArithmetic() {
        DefaultArithmetic arithmetic = new DefaultArithmetic();
        DatawaveJexlEngine engine = new DatawaveJexlEngine(builder(arithmetic));
        JexlUberspect uberspect = engine.getUberspect();
        JexlOperator.Uberspect operators = uberspect.getOperator(arithmetic);
        assertSame(operators, uberspect.getOperator(arithmetic));

        DefaultArithmetic requestArithmetic = new DefaultArithmetic();
        assertNotSame(operators, uberspect.getOperator(requestArithmetic));
        assertNotSame(uberspect.getOperator(requestArithmetic), uberspect.getOperator(requestArithmetic));
        assertSame(operators, uberspect.getOperator(arithmetic));
    }

    @Test
    void isolatesEnginesWhenABuilderIsReused() {
        DefaultArithmetic arithmetic = new DefaultArithmetic();
        JexlBuilder builder = builder(arithmetic).cache(1024);
        DatawaveJexlEngine first = new DatawaveJexlEngine(builder);
        DatawaveJexlEngine second = new DatawaveJexlEngine(builder.cache(0));
        assertNotSame(first.getUberspect(), second.getUberspect());
        assertNotSame(first.getUberspect().getOperator(arithmetic), second.getUberspect().getOperator(arithmetic));
    }

    @Test
    void retainsAnExplicitUberspect() {
        JexlUberspect custom = Engine.getUberspect(null, null, JexlPermissions.UNRESTRICTED);
        DatawaveJexlEngine engine = new DatawaveJexlEngine(builder(new DefaultArithmetic()).uberspect(custom));
        assertSame(custom, engine.getUberspect());
    }

    @Test
    void isolatesRequestArithmeticOptionsFromTheEngineDispatcher() {
        DatawaveJexlEngine engine = new DatawaveJexlEngine(builder(new DefaultArithmetic()));
        JexlScript script = engine.createScript("null + 1");
        assertEquals(1, script.execute(new MapContext()));

        OptionsContext strict = new OptionsContext();
        strict.options.setStrictArithmetic(true);
        assertThrows(JexlException.class, () -> script.execute(strict));

        assertEquals(1, script.execute(new MapContext()));
        strict.options.setStrictArithmetic(false);
        assertEquals(1, script.execute(strict));
    }

    @Test
    void invalidatesTheDispatcherWhenTheClassLoaderChanges() {
        DefaultArithmetic arithmetic = new DefaultArithmetic();
        DatawaveJexlEngine engine = new DatawaveJexlEngine(builder(arithmetic).logger(LogFactory.getLog(DatawaveJexlUberspectTest.class)));
        JexlUberspect uberspect = engine.getUberspect();
        JexlOperator.Uberspect operators = uberspect.getOperator(arithmetic);
        ClassLoader loader = new ClassLoader(uberspect.getClassLoader()) {};
        engine.setClassLoader(loader);
        assertSame(loader, uberspect.getClassLoader());
        assertNotSame(operators, uberspect.getOperator(arithmetic));
    }

    @ParameterizedTest
    @ValueSource(strings = {"A + B", "A == B", "A != B"})
    void retainsTypedOperatorOverloads(String query) {
        Object expected = query.equals("A + B") ? 132 : query.equals("A == B");
        assertMatchesStock(query, new TypedArithmetic(), expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"A == B", "A != B", "A < B", "A > B", "A <= B", "A >= B"})
    void retainsCompareOverloads(String query) {
        boolean expected = query.equals("A == B") || query.equals("A <= B") || query.equals("A >= B");
        assertMatchesStock(query, new CompareArithmetic(), expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"null + B", "null < B"})
    void retainsStrictNullFailures(String query) {
        JexlEngine stock = builder(new JexlArithmetic(true)).create();
        JexlEngine candidate = new DatawaveJexlEngine(builder(new JexlArithmetic(true)));
        MapContext context = new MapContext();
        context.set("B", 1);
        JexlInfo info = new JexlInfo("operator", 1, 1);
        JexlException expected = assertThrows(JexlException.class, () -> stock.createScript(info, query).execute(context));
        JexlException actual = assertThrows(JexlException.class, () -> candidate.createScript(info, query).execute(context));
        assertEquals(expected.getClass(), actual.getClass());
        assertEquals(expected.getMessage(), actual.getMessage());
        assertEquals(expected.getCause() == null ? null : expected.getCause().getClass(), actual.getCause() == null ? null : actual.getCause().getClass());
    }

    private void assertMatchesStock(String query, JexlArithmetic arithmetic, Object initialResult) {
        JexlEngine stock = builder(arithmetic).create();
        JexlEngine candidate = new DatawaveJexlEngine(builder(arithmetic));
        MapContext context = new MapContext();
        context.set("A", 11);
        context.set("B", 21);
        // Reuse scripts so operator-cache hits are exercised as well as initial dispatch.
        JexlScript expected = stock.createScript(query);
        JexlScript actual = candidate.createScript(query);
        assertEquals(initialResult, expected.execute(context));
        assertEquals(expected.execute(context), actual.execute(context));
        context.set("B", 12);
        assertEquals(expected.execute(context), actual.execute(context));
    }

    private JexlBuilder builder(JexlArithmetic arithmetic) {
        return new JexlBuilder().arithmetic(arithmetic).permissions(JexlPermissions.UNRESTRICTED).strict(true).cache(1024);
    }

    private static class OptionsContext extends MapContext implements JexlContext.OptionsHandle {
        private final JexlOptions options = new JexlOptions();

        @Override
        public JexlOptions getEngineOptions() {
            return options;
        }
    }

    public static class TypedArithmetic extends JexlArithmetic {
        public TypedArithmetic() {
            super(false);
        }

        public int add(Integer left, Integer right) {
            return left + right + 100;
        }

        public boolean equals(Integer left, Integer right) {
            return left % 10 == right % 10;
        }
    }

    public static class CompareArithmetic extends JexlArithmetic {
        public CompareArithmetic() {
            super(false);
        }

        public int compare(Integer left, Integer right) {
            return Integer.compare(left % 10, right % 10);
        }
    }
}
