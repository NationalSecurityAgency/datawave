package datawave.query.jexl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.internal.Script;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.attributes.Content;
import datawave.query.attributes.Document;

public class DatawaveJexlEngineTest {

    private DatawaveJexlEngine engine = null;

    @BeforeEach
    public void setup() {
        engine = ArithmeticJexlEngines.getEngine(new HitListArithmetic());
    }

    @Test
    public void testScriptWithComplexFeatures() {
        Key key = new Key("row", "dt\0uid");

        Document doc = new Document();
        doc.put("FIELD_A", new Content("value", key, false));
        doc.put("FIELD_B", new Content("value", key, false));

        DatawaveJexlContext context = new DatawaveJexlContext();
        doc.visit(List.of("FIELD_A", "FIELD_B"), context);

        // for complex script features use the DatawaveJexlEngine#createComplexScript()
        String query = "if (FIELD_A == FIELD_B) FIELD_A = 'repeat';";
        Script script = engine.createComplexScript(query);
        Object result = script.execute(context);

        assertInstanceOf(String.class, result);
        assertEquals("repeat", String.valueOf(result));

        assertEquals("value", doc.get("FIELD_A").getData());
        assertEquals("repeat", context.get("FIELD_A"));
    }

    @Test
    public void testScriptWithComplexFeaturesNotSupported() {
        // complex jexl features are disabled via DatawaveJexlEngine#createScript()
        String query = "if (FIELD_A == FIELD_B) FIELD_A = 'repeat';";
        assertThrows(JexlException.class, () -> engine.createScript(query));
    }
}
