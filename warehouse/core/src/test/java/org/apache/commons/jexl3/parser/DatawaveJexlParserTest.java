package org.apache.commons.jexl3.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.JexlInfo;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DatawaveJexlParserTest {

    @ParameterizedTest
    @ValueSource(strings = {"FIELD == 'value'", "FIELD == 'value'   ", "FIELD == 'value' // comment", "FIELD == 'value'\n// comment\n",
            "FIELD == 'value' /* comment */", "FIELD == 'value'\r\n&& true", "FIELD == 'val\\'ue'", "FIELD == '\\\\value'", "", " ", "// comment",
            "var total = 0; for (var i : [1, 2, 3]) { total += i; } total", "var f = (x) -> { x + 1 }; f(2)", "{'a': 1, 'b': 2}['b']", "[1, 2, 3][1]",
            "FIELD == 'value' ? 1 : 2", "var a = 1; a += 2; a", "size([1, 2, 3])"})
    void preservesEndOfInputBehavior(String query) {
        MapContext context = new MapContext();
        context.set("FIELD", "value");
        JexlEngine stock = new JexlBuilder().create();
        JexlEngine candidate = DatawaveJexlParser.configure(new JexlBuilder()).create();
        assertEquals(stock.createScript(query).execute(context), candidate.createScript(query).execute(context));
    }

    @ParameterizedTest
    @ValueSource(strings = {"FIELD ==", "(FIELD == 'value'", "FIELD == 'value' &&", "FIELD == 'unterminated", "FIELD == 'value'\n&&", "FIELD == 'value' /*"})
    void preservesMalformedInputDiagnostics(String query) {
        JexlInfo info = new JexlInfo("query", 1, 1);
        JexlFeatures features = new JexlFeatures();
        JexlException expected = assertThrows(JexlException.class, () -> new Parser(new StringProvider(";")).parse(info, features, query, null));
        JexlException actual = assertThrows(JexlException.class, () -> DatawaveJexlParser.create().parse(info, features, query, null));
        assertEquals(expected.getClass(), actual.getClass());
        assertEquals(expected.getMessage(), actual.getMessage());
        assertEquals(expected.getInfo().getLine(), actual.getInfo().getLine());
        assertEquals(expected.getInfo().getColumn(), actual.getInfo().getColumn());
    }

    @Test
    void handlesBufferExpansionAndReuseAfterErrors() {
        JexlEngine engine = DatawaveJexlParser.configure(new JexlBuilder().cache(0)).create();
        String value = "x".repeat(20_000) + "\uD83D\uDE00";
        MapContext context = new MapContext();
        context.set("FIELD", value);
        JexlScript first = engine.createScript("FIELD == '" + value + "'");
        assertEquals(Boolean.TRUE, first.execute(context));
        assertThrows(JexlException.class, () -> engine.createScript("FIELD == '" + value));
        assertEquals(Boolean.FALSE, engine.createScript("FIELD == 'other'").execute(context));
        assertEquals(Boolean.TRUE, first.execute(context));
        assertEquals(Boolean.TRUE, engine.createScript("FIELD == '" + value + "'").execute(context));
        assertEquals(Boolean.TRUE, engine.createScript("(".repeat(40) + "FIELD == '" + value + "'" + ")".repeat(40)).execute(context));
    }
}
