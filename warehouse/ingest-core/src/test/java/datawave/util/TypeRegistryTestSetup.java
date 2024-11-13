package datawave.util;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;

public class TypeRegistryTestSetup {
    public static TypeRegistry resetTypeRegistry(Configuration conf) {
        TypeRegistry.reset();
        return TypeRegistry.getInstance(conf);
    }

    public static TypeRegistry resetTypeRegistryWithTypes(Configuration conf, Type... types) {
        TypeRegistry registry = resetTypeRegistry(conf);
        for (Type type : types) {
            registry.put(type.typeName(), type);
        }
        return registry;
    }
}
