package datawave.microservice.accumulo.stats;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import datawave.webservice.response.StatsResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Stats Controller /v1", description = "DataWave Stats Operations",
                externalDocs = @ExternalDocumentation(description = "Accumulo Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-accumulo-service"))
@RestController
@Secured({"InternalUser", "Administrator"})
@RequestMapping(path = "/v1", produces = {MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
@ConditionalOnProperty(name = "accumulo.stats.enabled", havingValue = "true", matchIfMissing = true)
public class StatsController {
    
    private final StatsService statsService;
    
    @Autowired
    public StatsController(StatsService statsService) {
        this.statsService = statsService;
    }
    
    /**
     * Retrieves stats from the Accumulo monitor
     *
     * @return a StatsResponse
     */
    @Operation(summary = "Retrieves statistics from the Accumulo monitor")
    @RequestMapping(path = "/stats", method = {RequestMethod.GET})
    public StatsResponse accumuloStats() {
        return statsService.getStats();
    }
}
