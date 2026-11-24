package datawave.ingest.mapreduce.handler.shard;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.visibility.AccessExpressionUtil;

/**
 * Verifies the visibility bytes {@link ShardedDataTypeHandler#getVisibility} writes for a field. These bytes are stored in Accumulo keys, so they must not
 * change as a result of moving from field markings to field visibilities.
 */
public class ShardedDataTypeHandlerVisibilityTest {

    private static final String EVENT_VISIBILITY = "EVENT&(PUBLIC|PRIVATE)";

    private final ShardedDataTypeHandler<Text> handler = new AbstractColumnBasedHandler<>();

    private static RawRecordContainerImpl event() {
        RawRecordContainerImpl event = new RawRecordContainerImpl();
        event.setVisibility(new ColumnVisibility(EVENT_VISIBILITY));
        return event;
    }

    /**
     * The bytes previously produced for a field with markings: the markings were converted to a normalized {@link ColumnVisibility} and then flattened.
     */
    private static byte[] legacyFieldVisibilityBytes(String expression) {
        return AccessExpressionUtil.toColumnVisibility(AccessExpressionUtil.normalize(expression)).flatten();
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {"A;A", "B&A;A&B", "A&(B&C);A&B&C", "(Z&Y)|(C&B);(B&C)|(Y&Z)", "X&Y&X;X&Y", "(((ABC)|(XYZ)));ABC|XYZ",
            "PUBLIC&FIELDMARK1;FIELDMARK1&PUBLIC", "(PUBLIC|PRIVATE)&FIELDMARK3;FIELDMARK3&(PRIVATE|PUBLIC)"})
    public void fieldVisibilityBytes(String expression, String expected) {
        NormalizedFieldAndValue value = new NormalizedFieldAndValue("FIELD", "value", new ColumnVisibility(expression));
        byte[] actual = handler.getVisibility(event(), value);

        assertEquals(expected, new String(actual, UTF_8));
        assertArrayEquals(legacyFieldVisibilityBytes(expression), actual);
        assertArrayEquals(new ColumnVisibility(expression).flatten(), actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"A\"&B", "\"A B\"&C", "\"a:b\"|\"c/d\""})
    public void quotedFieldVisibilityBytesMatchLegacy(String expression) {
        NormalizedFieldAndValue value = new NormalizedFieldAndValue("FIELD", "value", new ColumnVisibility(expression));
        assertArrayEquals(legacyFieldVisibilityBytes(expression), handler.getVisibility(event(), value));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void missingOrEmptyFieldVisibilityUsesEventVisibility(boolean empty) {
        NormalizedFieldAndValue value = new NormalizedFieldAndValue("FIELD", "value", empty ? new ColumnVisibility() : null);
        byte[] actual = handler.getVisibility(event(), value);

        assertArrayEquals(new ColumnVisibility(EVENT_VISIBILITY).flatten(), actual);
        assertEquals("EVENT&(PRIVATE|PUBLIC)", new String(actual, UTF_8));
    }
}
