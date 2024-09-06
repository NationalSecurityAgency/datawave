package datawave.microservice.map;

import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.map.config.MapServiceProperties;
import datawave.microservice.map.data.GeoFeatures;
import datawave.microservice.map.data.GeoQueryFeatures;
import datawave.webservice.query.exception.QueryException;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class MapController {
    private MapOperationsService mapOperationsService;
    private MapServiceProperties mapServiceProperties;
    
    public MapController(MapOperationsService mapOperationsService, MapServiceProperties mapServiceProperties) {
        this.mapOperationsService = mapOperationsService;
        this.mapServiceProperties = mapServiceProperties;
    }
    
    @RequestMapping(path = "/getGeoFeaturesForQuery", method = {RequestMethod.POST})
    public GeoQueryFeatures getGeoFeaturesForQuery(@RequestParam("plan") String plan, @RequestParam("fieldTypes") List<String> fieldTypes,
                    @RequestParam(value = "expand", required = false) boolean expand, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return mapOperationsService.getGeoFeaturesForQuery(plan, fieldTypes, expand, currentUser);
    }
    
    @RequestMapping(path = "/getGeoFeaturesForQueryId", method = {RequestMethod.POST})
    public GeoQueryFeatures getGeoFeaturesForQueryId(@RequestParam("queryId") String queryId, @AuthenticationPrincipal DatawaveUserDetails currentUser)
                    throws QueryException {
        return mapOperationsService.getGeoFeaturesForQueryId(queryId, currentUser);
    }
    
    @RequestMapping(path = "/geoFeaturesForGeometry", method = {RequestMethod.POST})
    public GeoFeatures geoFeaturesForGeometry(@RequestParam("geometry") String geometry,
                    @RequestParam(value = "createRanges", required = false, defaultValue = "false") Boolean createRanges,
                    @RequestParam(value = "rangeType", required = false) String rangeType,
                    @RequestParam(value = "maxEnvelopes", required = false) Integer maxEnvelopes,
                    @RequestParam(value = "maxExpansion", required = false) Integer maxExpansion,
                    @RequestParam(value = "optimizeRanges", required = false, defaultValue = "false") Boolean optimizeRanges,
                    @RequestParam(value = "rangeSplitThreshold", required = false) Integer rangeSplitThreshold,
                    @RequestParam(value = "maxRangeOverlap", required = false) Double maxRangeOverlap,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return mapOperationsService.geoFeaturesForGeometry(geometry, createRanges, rangeType, maxEnvelopes, maxExpansion, optimizeRanges, rangeSplitThreshold,
                        maxRangeOverlap, currentUser);
    }
    
    @RequestMapping(path = "/basemaps", method = {RequestMethod.GET})
    public List<MapServiceProperties.Basemap> basemaps() {
        return mapServiceProperties.getBasemaps();
    }
    
    @RequestMapping(path = "/header", method = {RequestMethod.GET})
    public MapServiceProperties.Banner header() {
        return mapServiceProperties.getHeader();
    }
    
    @RequestMapping(path = "/footer", method = {RequestMethod.GET})
    public MapServiceProperties.Banner footer() {
        return mapServiceProperties.getFooter();
    }
}
