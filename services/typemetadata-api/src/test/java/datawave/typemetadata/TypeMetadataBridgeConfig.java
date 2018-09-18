package datawave.typemetadata;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.hdfs.HdfsFileProvider;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

@Configuration
@PropertySource("classpath:typemetadata.properties")
public class TypeMetadataBridgeConfig {
    
    @Bean
    public ConversionService conversionService() {
        return new DefaultConversionService();
    }
    
    @Value("${type.metadata.table.names}")
    private String[] metadataTableNames;
    
    @Value("${type.metadata.hdfs.uri}")
    private String uri;
    
    @Value("${type.metadata.dir}")
    private String dir;
    
    @Value("${type.metadata.fileName}")
    private String fileName;
    
    @Bean
    public DefaultFileSystemManager vfs() throws FileSystemException {
        DefaultFileSystemManager vfs = new DefaultFileSystemManager();
        vfs.addProvider("hdfs", hdfsFileProvider());
        vfs.addProvider("file", defaultFileProvider());
        vfs.init();
        return vfs;
    }
    
    @Bean
    public HdfsFileProvider hdfsFileProvider() {
        return new HdfsFileProvider();
    }
    
    @Bean
    public DefaultLocalFileProvider defaultFileProvider() {
        return new DefaultLocalFileProvider();
    }
    
    @Bean
    public TypeMetadataBridge typeMetadataBridge() throws Exception {
        return TypeMetadataBridge.builder().withTableNames(metadataTableNames).withUri(uri).withDir(dir).withFileSystemManager(vfs()).withFileName(fileName)
                        .build().init();
    }
    
    @Bean
    public static PropertySourcesPlaceholderConfigurer pcc() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}
