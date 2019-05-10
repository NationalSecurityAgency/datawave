package datawave.microservice.accumulo.stats;

import datawave.webservice.response.StatsResponse;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;

@RestController
@RolesAllowed({"InternalUser", "Administrator"})
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
     * @return {@link StatsResponse}
     */
    @ApiOperation(value = "Retrieves statistics from the Accumulo monitor")
    @RequestMapping(path = "/stats", method = {RequestMethod.GET})
    public StatsResponse accumuloStats() {
        return statsService.getStats();
    }
}
