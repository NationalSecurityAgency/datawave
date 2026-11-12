package datawave.query.jexl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.VariableNameVisitor;

class JexlIdentifierCompatibilityTest {

    @ParameterizedTest
    @ValueSource(strings = {"try", "catch", "finally", "throw", "instanceof"})
    void preservesPreviouslyValidFieldNames(String field) throws Exception {
        String query = field + " == 'value'";
        ASTJexlScript tree = JexlASTHelper.parseAndFlattenJexlQuery(query);
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(tree));
        assertEquals(Set.of(field), VariableNameVisitor.parseQuery(query));
        assertEquals(PrintingVisitor.formattedQueryString(tree), PrintingVisitor.formattedQueryString(query, 10, 10));

        DatawaveJexlEngine engine = ArithmeticJexlEngines.getEngine(new DefaultArithmetic());
        assertEquals(query, JexlStringBuildingVisitor.buildQuery(engine.parse(query)));
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set(field, "value");
        assertTrue(ArithmeticJexlEngines.isMatched(engine.createScript(query).execute(context)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"try", "catch", "finally", "throw", "instanceof"})
    void preservesPreviouslyValidLocalVariables(String name) {
        DatawaveJexlEngine engine = ArithmeticJexlEngines.getEngine(new DefaultArithmetic());
        assertEquals(Boolean.TRUE, engine.createComplexScript("var " + name + " = 'value'; " + name + " == 'value'").execute(new DatawaveJexlContext()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"try", "catch", "finally", "throw", "instanceof"})
    void preservesPreviouslyValidNamespaceNames(String name) {
        DatawaveJexlEngine engine = new DatawaveJexlEngine(new JexlBuilder().arithmetic(new DefaultArithmetic()).permissions(JexlPermissions.UNRESTRICTED)
                        .namespaces(Map.of(name, new Functions())));
        assertEquals(Boolean.TRUE, engine.createScript(name + ":matches('value')").execute(new DatawaveJexlContext()));
    }

    public static class Functions {
        public boolean matches(String value) {
            return "value".equals(value);
        }
    }
}
