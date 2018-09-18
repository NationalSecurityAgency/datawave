package datawave.typemetadata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "datawave.typemetadata")
public class TypeMetadataWriterConfig {
    
    @Autowired
    private TypeMetadataBridge typeMetadataBridge;
    
    @Bean
    public TypeMetadataWriter typeMetadataWriter() {
        return TypeMetadataWriter.builder().withBridge(typeMetadataBridge).build();
    }
}
