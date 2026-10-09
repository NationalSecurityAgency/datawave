package datawave.query.language.functions.jexl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SummaryOptionsTest {

    @Test
    void testValidateWithNoParameters() {
        SummaryOptions summaryOptions = new SummaryOptions();
        assertDoesNotThrow(summaryOptions::validate);
    }

    @Test
    void testEmptyOptionsUseDefaultSize() {
        datawave.query.attributes.SummaryOptions summaryOptions = datawave.query.attributes.SummaryOptions.from("");
        assertEquals(150, summaryOptions.getSummarySize());
    }
}
