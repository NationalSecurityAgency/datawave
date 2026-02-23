package datawave.accumulo.inmemory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class AccumuloValidatorsTest {

    // NEW_TABLE_NAME tests

    @Test
    public void testNewTableNameValid() {
        assertDoesNotThrow(() -> AccumuloValidators.NEW_TABLE_NAME.validate("myTable"));
    }

    @Test
    public void testNewTableNameValidWithUnderscore() {
        assertDoesNotThrow(() -> AccumuloValidators.NEW_TABLE_NAME.validate("my_table"));
    }

    @Test
    public void testNewTableNameValidWithDigits() {
        assertDoesNotThrow(() -> AccumuloValidators.NEW_TABLE_NAME.validate("table123"));
    }

    @Test
    public void testNewTableNameValidQualified() {
        assertDoesNotThrow(() -> AccumuloValidators.NEW_TABLE_NAME.validate("namespace.table"));
    }

    @Test
    public void testNewTableNameNull() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_TABLE_NAME.validate(null));
    }

    @Test
    public void testNewTableNameEmpty() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_TABLE_NAME.validate(""));
    }

    @Test
    public void testNewTableNameStartsWithDot() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_TABLE_NAME.validate(".table"));
    }

    @Test
    public void testNewTableNameTooLong() {
        String longName = "a".repeat(1025);
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_TABLE_NAME.validate(longName));
    }

    @Test
    public void testNewTableNameExactly1024() {
        String name = "a".repeat(1024);
        assertDoesNotThrow(() -> AccumuloValidators.NEW_TABLE_NAME.validate(name));
    }

    @Test
    public void testNewTableNameBlankNamespacePart() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_TABLE_NAME.validate(".table"));
    }

    @Test
    public void testNewTableNameBlankTablePart() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_TABLE_NAME.validate("namespace."));
    }

    @Test
    public void testNewTableNameInvalidChars() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_TABLE_NAME.validate("my-table"));
    }

    @Test
    public void testNewTableNameInvalidCharsInNamespace() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_TABLE_NAME.validate("name space.table"));
    }

    @Test
    public void testNewTableNameInvalidCharsInTablePart() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_TABLE_NAME.validate("namespace.my table"));
    }

    // NEW_NAMESPACE_NAME tests

    @Test
    public void testNewNamespaceNameValid() {
        assertDoesNotThrow(() -> AccumuloValidators.NEW_NAMESPACE_NAME.validate("myNamespace"));
    }

    @Test
    public void testNewNamespaceNameValidWithUnderscore() {
        assertDoesNotThrow(() -> AccumuloValidators.NEW_NAMESPACE_NAME.validate("my_namespace"));
    }

    @Test
    public void testNewNamespaceNameNull() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_NAMESPACE_NAME.validate(null));
    }

    @Test
    public void testNewNamespaceNameEmpty() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_NAMESPACE_NAME.validate(""));
    }

    @Test
    public void testNewNamespaceNameTooLong() {
        String longName = "a".repeat(1025);
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_NAMESPACE_NAME.validate(longName));
    }

    @Test
    public void testNewNamespaceNameExactly1024() {
        String name = "a".repeat(1024);
        assertDoesNotThrow(() -> AccumuloValidators.NEW_NAMESPACE_NAME.validate(name));
    }

    @Test
    public void testNewNamespaceNameInvalidChars() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.NEW_NAMESPACE_NAME.validate("my-namespace"));
    }

    // EXISTING_NAMESPACE_NAME tests

    @Test
    public void testExistingNamespaceNameValid() {
        assertDoesNotThrow(() -> AccumuloValidators.EXISTING_NAMESPACE_NAME.validate("myNamespace"));
    }

    @Test
    public void testExistingNamespaceNameEmpty() {
        assertDoesNotThrow(() -> AccumuloValidators.EXISTING_NAMESPACE_NAME.validate(""));
    }

    @Test
    public void testExistingNamespaceNameNull() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.EXISTING_NAMESPACE_NAME.validate(null));
    }

    @Test
    public void testExistingNamespaceNameTooLong() {
        String longName = "a".repeat(1025);
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.EXISTING_NAMESPACE_NAME.validate(longName));
    }

    @Test
    public void testExistingNamespaceNameExactly1024() {
        String name = "a".repeat(1024);
        assertDoesNotThrow(() -> AccumuloValidators.EXISTING_NAMESPACE_NAME.validate(name));
    }

    @Test
    public void testExistingNamespaceNameInvalidChars() {
        assertThrows(IllegalArgumentException.class, () -> AccumuloValidators.EXISTING_NAMESPACE_NAME.validate("my-namespace"));
    }

    // Validator class tests

    @Test
    public void testValidatorThrowsWithDescriptiveMessage() {
        try {
            AccumuloValidators.NEW_TABLE_NAME.validate("");
        } catch (IllegalArgumentException e) {
            assert e.getMessage().contains("new table name");
        }
    }

    // Constructor test

    @Test
    public void testConstructorThrowsUnsupportedOperationException() {
        java.lang.reflect.InvocationTargetException ex = assertThrows(java.lang.reflect.InvocationTargetException.class, () -> {
            java.lang.reflect.Constructor<AccumuloValidators> constructor = AccumuloValidators.class.getDeclaredConstructor();
            constructor.setAccessible(true);
            constructor.newInstance();
        });
        assert ex.getCause() instanceof UnsupportedOperationException;
    }
}
