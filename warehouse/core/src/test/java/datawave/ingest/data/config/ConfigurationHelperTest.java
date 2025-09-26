package datawave.ingest.data.config;

import java.util.List;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConfigurationHelperTest {

    /**
     * Verify that if no match is found for "test.instance.1", that an empty list is returned.
     */
    @Test
    void testGetIncrementalInstancesWithNoMatchFound() {
        Configuration conf = new Configuration();
        List<TestInterface> actual = ConfigurationHelper.getIndexedInstances(conf, "test.instance", TestInterface.class, 1);
        Assertions.assertTrue(actual.isEmpty());
    }

    /**
     * Verify that all incremental instances of {@link ValidImpl} defined in the configuration are returned.
     */
    @Test
    void testGetIncrementalInstancesWithMatchFound() {
        Configuration conf = new Configuration();
        conf.set("test.instance.1", ValidImpl.class.getName());
        conf.set("test.instance.1.name", "FOO");
        conf.set("test.instance.2", ValidImpl.class.getName());
        conf.set("test.instance.2.name", "BAR");
        conf.set("test.instance.3", ValidImpl.class.getName());
        conf.set("test.instance.3.name", "HAT");

        List<TestInterface> expected = List.of(new ValidImpl("FOO"), new ValidImpl("BAR"), new ValidImpl("HAT"));
        List<TestInterface> actual = ConfigurationHelper.getIndexedInstances(conf, "test.instance", TestInterface.class, 1);
        Assertions.assertEquals(expected, actual);
    }

    /**
     * Verify that when the starting instance number is 3, that the instances "test.instance.1" and "test.instance.2" are not included in the returned list.
     */
    @Test
    void testGetIncrementalInstancesWithStartingIncrementOfThree() {
        Configuration conf = new Configuration();
        conf.set("test.instance.1", ValidImpl.class.getName());
        conf.set("test.instance.1.name", "FOO");
        conf.set("test.instance.2", ValidImpl.class.getName());
        conf.set("test.instance.2.name", "BAR");
        conf.set("test.instance.3", ValidImpl.class.getName());
        conf.set("test.instance.3.name", "HAT");
        conf.set("test.instance.4", ValidImpl.class.getName());
        conf.set("test.instance.4.name", "BOT");

        List<TestInterface> expected = List.of(new ValidImpl("HAT"), new ValidImpl("BOT"));
        List<TestInterface> actual = ConfigurationHelper.getIndexedInstances(conf, "test.instance", TestInterface.class, 3);
        Assertions.assertEquals(expected, actual);
    }

    /**
     * Verify that an error is thrown if a configured instance cannot be assigned to the specified list element type.
     */
    @Test
    void testGetIncrementalInstanceWithUnAssignableClass() {
        Configuration conf = new Configuration();
        conf.set("test.instance.1", String.class.getName());
        conf.set("test.instance.1.name", "BAR");

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                        () -> ConfigurationHelper.getIndexedInstances(conf, "test.instance", TestInterface.class, 1));
        Assertions.assertEquals("class java.lang.String cannot be cast to interface datawave.ingest.data.config.ConfigurationHelperTest$TestInterface",
                        exception.getMessage());
    }

    /**
     * Verify that an error is thrown if a configured instance type does not have a constructor that accepts a {@link Configuration} and {@link String}
     * parameter.
     */
    @Test
    void testGetIncrementalInstanceWithMissingConstructor() {
        Configuration conf = new Configuration();
        conf.set("test.instance.1", MissingConstructorImpl.class.getName());
        conf.set("test.instance.1.name", "FOO");

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                        () -> ConfigurationHelper.getIndexedInstances(conf, "test.instance", TestInterface.class, 1));
        Assertions.assertEquals("Failed to invoke constructor MissingConstructorImpl(class org.apache.hadoop.conf.Configuration, class java.lang.String)",
                        exception.getMessage());
        Assertions.assertInstanceOf(NoSuchMethodException.class, exception.getCause());
    }

    /**
     * Basic interface for testing {@link ConfigurationHelper#getIndexedInstances(Configuration, String, Class, int)}.
     */
    public interface TestInterface {}

    /**
     * Valid implementation of {@link TestInterface} with the requisite constructor.
     */
    public static class ValidImpl implements TestInterface {
        private final String name;

        public ValidImpl(String name) {
            this.name = name;
        }

        @SuppressWarnings("unused")
        public ValidImpl(Configuration conf, String propertyName) {
            this.name = conf.get((propertyName + ".name"));
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            ValidImpl valid = (ValidImpl) object;
            return Objects.equals(name, valid.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    /**
     * Invalid implementation of {@link TestInterface} missing the requisite constructor.
     */
    public static class MissingConstructorImpl implements TestInterface {
        public MissingConstructorImpl() {}
    }
}
