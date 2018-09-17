package datawave.typemetadata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

@Configuration
@ComponentScan(basePackages = "datawave.typemetadata")
public class TypeMetadataProviderConfig {
    
    @Bean
    public ConversionService conversionService() {
        return new DefaultConversionService();
    }
    
    @Value("${type.metadata.table.names}")
    private String[] tableNames;
    
    @Autowired
    private TypeMetadataBridge typeMetadataBridge;
    
    @Bean
    public TypeMetadataProvider typeMetadataProvider() {
        return TypeMetadataProvider.builder().withBridge(typeMetadataBridge).withTableNames(tableNames).build().init();
    }
}
