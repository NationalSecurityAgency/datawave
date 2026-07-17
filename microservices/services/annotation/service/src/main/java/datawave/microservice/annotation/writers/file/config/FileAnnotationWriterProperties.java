package datawave.microservice.annotation.writers.file.config;

import java.util.List;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotEmpty;

import org.springframework.validation.annotation.Validated;

import lombok.Getter;
import lombok.Setter;

@Validated
@Getter
@Setter
public class FileAnnotationWriterProperties {
    private String user;

    @NotEmpty
    private String pathUri;
    private String subPath;
    private String subPathEnvVar;
    private String prefix;
    private List<String> fsConfigResources;

    @DecimalMin("10")
    private Long maxFileLengthMB;

    @DecimalMin("60")
    private Long maxFileAgeSeconds;
}
