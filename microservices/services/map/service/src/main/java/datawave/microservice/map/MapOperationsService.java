package datawave.microservice.map;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.http.HttpHeaders;
import org.geotools.data.DataUtilities;
import org.geotools.geojson.geom.GeometryJSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import datawave.core.geo.utils.GeoQueryConfig;
import datawave.core.query.jexl.JexlASTHelper;
import datawave.data.type.GeoType;
import datawave.data.type.GeometryType;
import datawave.data.type.PointType;
import datawave.data.type.Type;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.map.config.MapServiceProperties;
import datawave.microservice.map.data.GeoFeature;
import datawave.microservice.map.data.GeoFeatures;
import datawave.microservice.map.data.GeoQueryFeatures;
import datawave.microservice.map.visitor.GeoFeatureVisitor;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.webservice.dictionary.data.DataDictionaryBase;
import datawave.webservice.metadata.MetadataFieldBase;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;

@Service
public class MapOperationsService {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final MapServiceProperties mapServiceProperties;
    private final WebClient webClient;
    private final JWTTokenHandler jwtTokenHandler;
    private final MetadataHelperFactory metadataHelperFactory;
    
    private final AccumuloClient accumuloClient;
    private final Set<String> geoFields = new HashSet<>();
    private final Set<String> geoWaveFields = new HashSet<>();
    
    public MapOperationsService(MapServiceProperties mapServiceProperties, WebClient.Builder webClientBuilder, JWTTokenHandler jwtTokenHandler,
                    MetadataHelperFactory metadataHelperFactory, @Qualifier("warehouse") AccumuloClient accumuloClient) {
        this.mapServiceProperties = mapServiceProperties;
        this.webClient = webClientBuilder.build();
        this.jwtTokenHandler = jwtTokenHandler;
        this.metadataHelperFactory = metadataHelperFactory;
        this.accumuloClient = accumuloClient;
        loadGeoFields();
    }
    
    public void loadGeoFields() {
        geoFields.clear();
        geoWaveFields.clear();
        geoFields.addAll(mapServiceProperties.getGeoFields());
        geoWaveFields.addAll(mapServiceProperties.getGeoWaveFields());
        loadGeoFieldsFromDictionary();
    }
    
    private void loadGeoFieldsFromDictionary() {
        // @formatter:off
        webClient
                .get()
                .uri(UriComponentsBuilder
                        .fromHttpUrl(mapServiceProperties.getDictionaryUri())
                        .toUriString())
                .header(HttpHeaders.AUTHORIZATION, createBearerHeader())
                .retrieve()
                .toEntity(DataDictionaryBase.class)
                .doOnError(e -> log.warn("Encountered error while attempting to load geo fields from dictionary", e))
                .doOnSuccess(response -> loadGeoFieldsFromDictionary(response.getBody()));
        // @formatter:on
    }
    
    private void loadGeoFieldsFromDictionary(DataDictionaryBase<?,?> dictionary) {
        if (dictionary != null && dictionary.getFields() != null) {
            for (MetadataFieldBase<?,?> field : dictionary.getFields()) {
                boolean geoField = false;
                boolean geowaveField = false;
                for (String type : field.getTypes()) {
                    if (mapServiceProperties.getGeoTypes().contains(type)) {
                        geoFields.add(field.getFieldName());
                        geoField = true;
                    }
                    if (mapServiceProperties.getGeoWaveTypes().contains(type)) {
                        geoWaveFields.add(field.getFieldName());
                        geowaveField = true;
                    }
                    if (geoField && geowaveField) {
                        break;
                    }
                }
            }
        }
    }
    
    // We could save the jwt token for reuse, but it will eventually expire. Since this should be used infrequently (i.e. when loading the dictionary) let's
    // generate it each time.
    protected String createBearerHeader() {
        final String jwt;
        try {
            // @formatter:off
            jwt = webClient.get()
                    .uri(URI.create(mapServiceProperties.getAuthorizationUri()))
                    .retrieve()
                    .bodyToMono(String.class)
                    .block(Duration.ofSeconds(30));
            // @formatter:on
        } catch (IllegalStateException e) {
            throw new IllegalStateException("Timed out waiting for remote authorization response", e);
        }
        
        Collection<DatawaveUser> principals = jwtTokenHandler.createUsersFromToken(jwt);
        long createTime = principals.stream().map(DatawaveUser::getCreationTime).min(Long::compareTo).orElse(System.currentTimeMillis());
        DatawaveUserDetails userDetails = new DatawaveUserDetails(principals, createTime);
        
        return "Bearer " + jwtTokenHandler.createTokenFromUsers(userDetails.getPrimaryUser().getName(), userDetails.getProxiedUsers());
    }
    
    public GeoQueryFeatures getGeoFeaturesForQuery(String plan, List<String> fieldTypes, boolean expand, DatawaveUserDetails currentUser) {
        ASTJexlScript script;
        try {
            script = JexlASTHelper.parseAndFlattenJexlQuery(plan);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        
        Set<Authorizations> userAuths = new HashSet<>();
        userAuths.add(new Authorizations(currentUser.getPrimaryUser().getAuths().toArray(new String[0])));
        
        Map<String,Set<Type<?>>> typesByField = new HashMap<>();
        try {
            // @formatter:off
            typesByField.putAll(metadataHelperFactory
                    .createMetadataHelper(accumuloClient, mapServiceProperties.getMetadataTableName(), userAuths)
                    .getFieldsToDatatypes(null)
                    .asMap()
                    .entrySet()
                    .stream()
                    .map(x -> Map.entry(x.getKey(), new LinkedHashSet<>(x.getValue())))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            // @formatter:on
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        if (fieldTypes != null) {
            fieldTypes.stream().flatMap(fieldType -> Arrays.stream(fieldType.split(","))).forEach(fieldType -> {
                String[] entry = fieldType.split(":");
                Class<?> clazz = null;
                try {
                    clazz = Class.forName(entry[1]);
                } catch (ClassNotFoundException e) {
                    if (log.isTraceEnabled()) {
                        log.trace("Class not found: {}", entry[1]);
                    }
                }
                
                if (clazz != null) {
                    if (Type.class.isAssignableFrom(clazz)) {
                        try {
                            Set<Type<?>> types = typesByField.computeIfAbsent(entry[0], k -> new HashSet<>());
                            types.add((Type<?>) clazz.getDeclaredConstructor().newInstance());
                        } catch (Exception e) {
                            if (log.isTraceEnabled()) {
                                log.trace("Unable to instantiate class: {}", clazz.getName());
                            }
                        }
                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("Class does not implement {}: {}", Type.class.getName(), clazz.getName());
                        }
                    }
                }
            });
        }
        
        GeoQueryConfig geoQueryConfig = (expand) ? GeoQueryConfig.builder().build() : null;
        return GeoFeatureVisitor.getGeoFeatures(script, typesByField, geoQueryConfig);
    }
    
    public GeoQueryFeatures getGeoFeaturesForQueryId(String queryId, DatawaveUserDetails currentUser) throws QueryException {
        BaseQueryMetricListResponse queryMetricResponse = loadQueryFromMetricsService(queryId, currentUser);
        
        GeoQueryFeatures geoQueryFeatures = null;
        if (queryMetricResponse.getResult().size() == 1) {
            BaseQueryMetric queryMetric = (BaseQueryMetric) queryMetricResponse.getResult().get(0);
            String query = queryMetric.getPlan();
            
            geoQueryFeatures = getGeoFeaturesForQuery(query, null, false, currentUser);
        }
        
        return geoQueryFeatures;
    }
    
    private BaseQueryMetricListResponse loadQueryFromMetricsService(String queryId, DatawaveUserDetails currentUser) throws QueryException {
        try {
            // @formatter:off
            ResponseEntity<BaseQueryMetricListResponse> responseEntity = webClient
                    .get()
                    .uri(UriComponentsBuilder
                            .fromHttpUrl(mapServiceProperties.getMetricsUri() + queryId)
                            .toUriString())
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + jwtTokenHandler.createTokenFromUsers(currentUser.getPrimaryUser().getName(), currentUser.getProxiedUsers()))
                    .retrieve()
                    .toEntity(BaseQueryMetricListResponse.class)
                    .doOnError(e -> log.warn("Encountered error while attempting to load query from query metric service", e))
                    .block(Duration.ofMillis(TimeUnit.SECONDS.toMillis(30)));
            // @formatter:on
            
            QueryException queryException;
            if (responseEntity != null) {
                BaseQueryMetricListResponse queryMetricListResponse = responseEntity.getBody();
                
                if (responseEntity.getStatusCode() == HttpStatus.OK) {
                    return queryMetricListResponse;
                } else {
                    if (queryMetricListResponse != null && queryMetricListResponse.getExceptions().size() > 0) {
                        QueryExceptionType exceptionType = queryMetricListResponse.getExceptions().get(0);
                        queryException = new QueryException(exceptionType.getCode(), exceptionType.getCause(), exceptionType.getMessage());
                    } else {
                        queryException = new QueryException("Unknown error occurred while retrieving query metrics for query " + queryId,
                                        responseEntity.getStatusCodeValue());
                    }
                }
            } else {
                queryException = new QueryException("Unknown error occurred while retrieving query metrics for query " + queryId);
            }
            throw queryException;
        } catch (Exception e) {
            log.error("Timed out waiting for query metrics response");
            throw new QueryException("Timed out waiting for query metrics response", e);
        }
    }
    
    public GeoFeatures geoFeaturesForGeometry(String geometry, String geometryType, Boolean createRanges, String rangeType, Integer maxEnvelopes,
                    Integer maxExpansion, Boolean optimizeRanges, Integer rangeSplitThreshold, Double maxRangeOverlap, DatawaveUserDetails currentUser) {
        String wkt = geometry;
        if (geometryType.equals("geojson")) {
            try {
                wkt = new GeometryJSON().read(wkt).toText();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        String field = "DEFAULT";
        Map<String,Set<Type<?>>> typesByField = new HashMap<>();
        GeoQueryConfig.Builder geoQueryConfigBuilder = GeoQueryConfig.builder();
        if (createRanges) {
            switch (rangeType) {
                case "GeoType":
                    typesByField.put(field, Collections.singleton(new GeoType()));
                    geoQueryConfigBuilder.setGeoMaxEnvelopes(maxEnvelopes).setGeoMaxExpansion(maxExpansion);
                    if (optimizeRanges != null) {
                        geoQueryConfigBuilder.setOptimizeGeoRanges(optimizeRanges);
                    }
                    break;
                case "PointType":
                    typesByField.put(field, Collections.singleton(new PointType()));
                    geoQueryConfigBuilder.setGeowaveMaxEnvelopes(maxEnvelopes).setPointMaxExpansion(maxExpansion);
                    if (optimizeRanges != null) {
                        geoQueryConfigBuilder.setOptimizeGeoWaveRanges(optimizeRanges);
                    }
                    if (rangeSplitThreshold != null) {
                        geoQueryConfigBuilder.setRangeSplitThreshold(rangeSplitThreshold);
                    }
                    if (maxRangeOverlap != null) {
                        geoQueryConfigBuilder.setMaxRangeOverlap(maxRangeOverlap);
                    }
                    break;
                case "GeometryType":
                    typesByField.put(field, Collections.singleton(new GeometryType()));
                    geoQueryConfigBuilder.setGeowaveMaxEnvelopes(maxEnvelopes).setGeometryMaxExpansion(maxExpansion);
                    if (optimizeRanges != null) {
                        geoQueryConfigBuilder.setOptimizeGeoWaveRanges(optimizeRanges);
                    }
                    if (rangeSplitThreshold != null) {
                        geoQueryConfigBuilder.setRangeSplitThreshold(rangeSplitThreshold);
                    }
                    if (maxRangeOverlap != null) {
                        geoQueryConfigBuilder.setMaxRangeOverlap(maxRangeOverlap);
                    }
                    break;
            }
        }
        String query = "geowave:intersects(" + field + ", \"" + wkt + "\")";
        
        ASTJexlScript script;
        try {
            script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        
        GeoQueryFeatures geoQueryFeatures = GeoFeatureVisitor.getGeoFeatures(script, typesByField, geoQueryConfigBuilder.build());
        
        GeoFeatures geoFeatures = new GeoFeatures();
        GeoFeature geoFeature = new GeoFeature();
        geoFeature.setWkt(wkt);
        geoFeature.setGeoJson(DataUtilities.collection(geoQueryFeatures.getFunctions().get(0).getGeoJson()));
        geoFeatures.setGeometry(geoFeature);
        geoFeatures.setQueryRanges(geoQueryFeatures.getGeoByField().get(field));
        
        return geoFeatures;
    }
    
    public Set<String> getGeoFields() {
        return geoFields;
    }
    
    public Set<String> getGeoWaveFields() {
        return geoWaveFields;
    }
}
