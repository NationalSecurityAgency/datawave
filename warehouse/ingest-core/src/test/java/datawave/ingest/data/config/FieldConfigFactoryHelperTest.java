package datawave.ingest.data.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.ingest.BaseIngestHelper;

public class FieldConfigFactoryHelperTest {
    @Test
    public void testLoadFactoryFromConfiguration() {
        Configuration conf = new Configuration();
        Type type = mock(Type.class);
        when(type.typeName()).thenReturn("test");
        conf.set("test.data.category.field.config.file.factory", TestFieldConfigurationHelperFactory.class.getName());

        FieldConfigHelperFactory factoryImpl = FieldConfigHelperFactory.loadFactory(type, conf);

        assertNotNull(factoryImpl);
        assertInstanceOf(TestFieldConfigurationHelperFactory.class, factoryImpl);
    }

    @Test
    public void testLoadFactoryReturnsDefaultFactoryWhenNotConfigured() {
        Configuration conf = new Configuration();
        Type type = mock(Type.class);
        when(type.typeName()).thenReturn("test");

        FieldConfigHelperFactory factoryImpl = FieldConfigHelperFactory.loadFactory(type, conf);

        assertNotNull(factoryImpl);
        assertInstanceOf(FieldConfigHelperFactory.DefaultFieldConfigHelperFactory.class, factoryImpl);
    }

    @Test
    public void testDefaultFactoryReturnsNullIfNotConfigured() {
        Configuration conf = new Configuration();
        Type type = mock(Type.class);
        FieldConfigHelperFactory factoryImpl = new FieldConfigHelperFactory.DefaultFieldConfigHelperFactory();
        BaseIngestHelper helper = mock(BaseIngestHelper.class);
        when(type.typeName()).thenReturn("test");
        when(helper.getType()).thenReturn(type);

        Optional<FieldConfigHelper> fieldHelper = factoryImpl.create(helper, conf);

        assertTrue(fieldHelper.isEmpty());
    }

    @Test
    public void testDefaultFactoryLoadsXmlFieldConfigHelper(@TempDir Path tempDir) throws Exception {
        Path fieldXmlPath = tempDir.resolve("field-cache.xml");
        try (InputStream in = ClassLoader.getSystemResourceAsStream("datawave/ingest/test-field-allowlist.xml");
             OutputStream out = new FileOutputStream(fieldXmlPath.toFile())) {
            assert in != null;
            IOUtils.copy(in, out);
        }
        Configuration conf = new Configuration();
        BaseIngestHelper helper = mock(BaseIngestHelper.class);
        Type type = mock(Type.class);

        when(type.typeName()).thenReturn("test");
        when(helper.getType()).thenReturn(type);
        conf.set("test.data.category.field.config.file", "file://" + fieldXmlPath);

        FieldConfigHelperFactory factoryImpl = FieldConfigHelperFactory.loadFactory(type, conf);
        Optional<FieldConfigHelper> fieldConfigHelper = factoryImpl.create(helper, conf);

        assertFalse(fieldConfigHelper.isEmpty());
        assertInstanceOf(XMLFieldConfigHelper.class, fieldConfigHelper.get());
    }

    public static class TestFieldConfigurationHelperFactory implements FieldConfigHelperFactory {
        @Override
        public Optional<FieldConfigHelper> create(BaseIngestHelper helper, Configuration conf) {
            return Optional.empty();
        }
    }
}
