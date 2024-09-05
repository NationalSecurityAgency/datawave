package datawave.microservice.map;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    
    // returns the fields mapped by type (geo vs geowave)
    @RequestMapping(path = "/fieldsByType", method = {RequestMethod.GET})
    public Map<String,Set<String>> fieldsByType() {
        Map<String,Set<String>> fieldsByType = new HashMap<>();
        fieldsByType.put("geo", mapOperationsService.getGeoFields());
        fieldsByType.put("geowave", mapOperationsService.getGeoWaveFields());
        return fieldsByType;
    }
    
    // reload fields from configuration and the data dictionary
    @RequestMapping(path = "/reloadFieldsByType", method = {RequestMethod.POST})
    public void reloadFieldsByType() {
        mapOperationsService.loadGeoFields();
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
}
