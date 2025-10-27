package datawave.annotation.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;

public class ValidatorTest {
    static class TestObject {
        String name;
        int size;
        LocalDate start;
        LocalDate end;
    }

    DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Test
    public void testSimpleValidation() {
        TestObject testObject = new TestObject();
        testObject.name = "Drew";
        testObject.size = 52;
        testObject.start = LocalDate.parse("2020-10-10", dateFormat);
        testObject.end = LocalDate.parse("2025-05-05", dateFormat);

        //@formatter:off
        Validator<TestObject> validator = Validator.<TestObject>create()
                .addCheck(o -> o.name != null && !o.name.isBlank(), "Name is required")
                .addCheck(o -> o.size > 0, "Size must be larger than zero")
                .addCheck(o -> o.start != null, "Start date must be null")
                .addCheck(o -> o.end != null, "End date must be null")
                .addCheck(o -> o.end.isAfter(o.start), "Start date must be prior to end date");
        //@formatter:on

        assertTrue(validator.check(testObject).isValid());
    }
}
