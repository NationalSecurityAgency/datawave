package datawave.ingest.data.config;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.ingest.BaseIngestHelper;

public interface FieldConfigHelperFactory {
    Optional<FieldConfigHelper> create(BaseIngestHelper helper, Configuration conf);

    static FieldConfigHelperFactory loadFactory(Type dataType, Configuration conf) {
        String typeName = dataType.typeName();
        String fieldConfigFactoryClassName = conf.get(typeName + ".data.category.field.config.file.factory", null);
        if (fieldConfigFactoryClassName == null) {
            return new DefaultFieldConfigHelperFactory();
        }
        try {
            // noinspection unchecked
            Class<FieldConfigHelperFactory> fieldConfigFactoryClass = (Class<FieldConfigHelperFactory>) Class.forName(fieldConfigFactoryClassName);
            return fieldConfigFactoryClass.getConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Unable to create field config factory class: " + fieldConfigFactoryClassName, e);
        }
    }

    class DefaultFieldConfigHelperFactory implements FieldConfigHelperFactory {
        private final static Logger log = LoggerFactory.getLogger(DefaultFieldConfigHelperFactory.class);

        public static final String FIELD_CONFIG_FILE = ".data.category.field.config.file";

        @Override
        public Optional<FieldConfigHelper> create(BaseIngestHelper helper, Configuration conf) {
            Type type = helper.getType();
            String typeName = type.typeName();
            String fieldConfigFile = conf.get(typeName + FIELD_CONFIG_FILE);
            if (fieldConfigFile == null) {
                return Optional.empty();
            }
            log.debug("Field config file {} specified for: {}{}", fieldConfigFile, typeName, FIELD_CONFIG_FILE);
            return Optional.ofNullable(XMLFieldConfigHelper.load(fieldConfigFile, helper));
        }
    }
}
