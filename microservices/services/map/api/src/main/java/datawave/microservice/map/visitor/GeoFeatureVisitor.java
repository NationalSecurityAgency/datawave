package datawave.microservice.map.visitor;

import static datawave.core.geo.utils.GeoUtils.GEO_NAMESPACE;
import static datawave.core.geo.utils.GeoWaveUtils.GEOWAVE_NAMESPACE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.geo.function.AbstractGeoFunctionDetails;
import datawave.core.geo.utils.CommonGeoUtils;
import datawave.core.geo.utils.GeoQueryConfig;
import datawave.core.geo.utils.GeoUtils;
import datawave.core.geo.utils.GeoWaveUtils;
import datawave.core.query.jexl.JexlASTHelper;
import datawave.core.query.jexl.LiteralRange;
import datawave.core.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.core.query.jexl.visitors.BaseVisitor;
import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.data.type.GeoType;
import datawave.data.type.GeometryType;
import datawave.data.type.PointType;
import datawave.data.type.Type;
import datawave.microservice.map.data.AbstractGeoTerms;
import datawave.microservice.map.data.GeoFeature;
import datawave.microservice.map.data.GeoFunctionFeature;
import datawave.microservice.map.data.GeoQueryFeatures;
import datawave.microservice.map.data.geo.GeoPointTerms;
import datawave.microservice.map.data.geowave.GeoWaveGeometryTerms;
import datawave.query.util.MetadataHelper;

/**
 * This visitor will traverse the query tree, and extract both the geo function and associated query geometry (as GeoJSON).
 */
public class GeoFeatureVisitor extends BaseVisitor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private static final String FEATURE_TYPE_NAME = "DatawaveFeature";
    private static final String FEATURE_SRS = "EPSG:4326";
    private static final String FUNCTIONS = "functions";
    private static final String FIELDS = "fields";
    private static final String TERM = "term";
    private static final String RANGE = "range";
    private static final String WKT = "wkt";
    private static final String GEO = "geo";
    private static final SimpleFeatureType featureType;
    private static final SimpleFeatureBuilder featureBuilder;
    
    private final MetadataHelper metadataHelper;
    private final Map<String,Set<Type<?>>> typesByField;
    private final GeoQueryConfig geoQueryConfig;
    private final GeoQueryFeatures geoQueryFeatures = new GeoQueryFeatures();
    
    private final Map<String,Map<Integer,List<String[]>>> geowaveGeometryByTierByField = new LinkedHashMap<>();
    private final Map<String,List<String[]>> geowavePointByField = new LinkedHashMap<>();
    private final Map<String,List<String[]>> geoPointByField = new LinkedHashMap<>();
    
    static {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName(FEATURE_TYPE_NAME);
        typeBuilder.setSRS(FEATURE_SRS);
        typeBuilder.add(FUNCTIONS, String[].class);
        typeBuilder.add(FIELDS, String[].class);
        typeBuilder.add(TERM, String.class);
        typeBuilder.add(RANGE, String[].class);
        typeBuilder.add(WKT, String.class);
        typeBuilder.add(GEO, Geometry.class);
        typeBuilder.setDefaultGeometry(GEO);
        featureType = typeBuilder.buildFeatureType();
        featureBuilder = new SimpleFeatureBuilder(featureType);
    }
    
    private GeoFeatureVisitor(MetadataHelper metadataHelper, Map<String,Set<Type<?>>> typesByField, GeoQueryConfig geoQueryConfig) {
        this.metadataHelper = metadataHelper;
        this.typesByField = typesByField;
        this.geoQueryConfig = geoQueryConfig;
    }
    
    public static GeoQueryFeatures getGeoFeatures(JexlNode node, Map<String,Set<Type<?>>> typesByField) {
        return getGeoFeatures(node, null, typesByField, null);
    }
    
    public static GeoQueryFeatures getGeoFeatures(JexlNode node, Map<String,Set<Type<?>>> typesByField, GeoQueryConfig geoQueryConfig) {
        return getGeoFeatures(node, null, typesByField, geoQueryConfig);
    }
    
    public static GeoQueryFeatures getGeoFeatures(JexlNode node, MetadataHelper metadataHelper) {
        return getGeoFeatures(node, metadataHelper, null, null);
    }
    
    public static GeoQueryFeatures getGeoFeatures(JexlNode node, MetadataHelper metadataHelper, GeoQueryConfig geoQueryConfig) {
        return getGeoFeatures(node, metadataHelper, null, geoQueryConfig);
    }
    
    public static GeoQueryFeatures getGeoFeatures(JexlNode node, MetadataHelper metadataHelper, Map<String,Set<Type<?>>> typesByField) {
        return getGeoFeatures(node, metadataHelper, typesByField, null);
    }
    
    public static GeoQueryFeatures getGeoFeatures(JexlNode node, MetadataHelper metadataHelper, Map<String,Set<Type<?>>> typesByField,
                    GeoQueryConfig geoQueryConfig) {
        GeoFeatureVisitor visitor = new GeoFeatureVisitor(metadataHelper, typesByField, geoQueryConfig);
        node.jjtAccept(visitor, null);
        visitor.generateGeoByField();
        return visitor.geoQueryFeatures;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        try {
            FunctionJexlNodeVisitor funcVis = FunctionJexlNodeVisitor.eval(node);
            AbstractGeoFunctionDetails geoFunction = null;
            switch (funcVis.namespace()) {
                case GEOWAVE_NAMESPACE:
                    geoFunction = GeoWaveUtils.parseGeoWaveFunction(funcVis.name(), funcVis.args());
                    break;
                case GEO_NAMESPACE:
                    geoFunction = GeoUtils.parseGeoFunction(funcVis.name(), funcVis.args());
                    break;
            }
            
            if (geoFunction != null) {
                // if we parsed the function successfully, save the fields and wkt
                String function = JexlStringBuildingVisitor.buildQuery(node);
                geoQueryFeatures.getFunctions().add(new GeoFunctionFeature(function, geoFunction.getFields(), createFunctionFeature(function, geoFunction)));
                
                if (geoQueryConfig != null) {
                    geoFunction.generateIndexNode(getTypesByField(geoFunction.getFields()), geoQueryConfig).jjtAccept(this, null);
                }
            }
        } catch (Exception e) {
            log.error("Unable to extract geo feature from function", e);
        }
        
        return node;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
        if (range != null && range.getLower() instanceof String) {
            @SuppressWarnings("unchecked")
            LiteralRange<String> stringRange = (LiteralRange<String>) range;
            
            Set<Type<?>> fieldTypes = getTypesForField(stringRange.getFieldName());
            if (fieldTypes != null) {
                for (Type<?> fieldType : fieldTypes) {
                    if (fieldType instanceof GeometryType) {
                        int tier = GeoWaveUtils.decodeTier(stringRange.getLower());
                        // @formatter:off
                        geowaveGeometryByTierByField
                                .computeIfAbsent(stringRange.getFieldName(), k -> new LinkedHashMap<>())
                                .computeIfAbsent(tier, k -> new ArrayList<>())
                                .add(new String[]{stringRange.getLower(), stringRange.getUpper()});
                        // @formatter:on
                    } else if (fieldType instanceof PointType) {
                        // @formatter:off
                        geowavePointByField
                                .computeIfAbsent(stringRange.getFieldName(), k -> new ArrayList<>())
                                .add(new String[] {stringRange.getLower(), stringRange.getUpper()});
                        // @formatter:on
                    } else if (fieldType instanceof GeoType) {
                        // @formatter:off
                        geoPointByField
                                .computeIfAbsent(stringRange.getFieldName(), k -> new ArrayList<>())
                                .add(new String[] {stringRange.getLower(), stringRange.getUpper()});
                        // @formatter:on
                    }
                }
            }
            
            return null;
        }
        
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        String fieldName = JexlNodes.getIdentifierOrLiteralAsString(node.jjtGetChild(0));
        
        Set<Type<?>> fieldTypes = getTypesForField(fieldName);
        if (fieldTypes != null) {
            String value = JexlNodes.getIdentifierOrLiteralAsString(JexlASTHelper.dereference(node.jjtGetChild(1)));
            for (Type<?> fieldType : fieldTypes) {
                if (fieldType instanceof GeometryType) {
                    int tier = GeoWaveUtils.decodeTier(value);
                    // @formatter:off
                    geowaveGeometryByTierByField
                            .computeIfAbsent(fieldName, k -> new LinkedHashMap<>())
                            .computeIfAbsent(tier, k -> new ArrayList<>()).add(new String[]{value});
                    // @formatter:on
                } else if (fieldType instanceof PointType) {
                    geowavePointByField.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(new String[] {value});
                } else if (fieldType instanceof GeoType) {
                    geoPointByField.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(new String[] {value});
                }
            }
        }
        
        return null;
    }
    
    private Map<String,Set<Type<?>>> getTypesByField(Collection<String> fields) {
        Map<String,Set<Type<?>>> typesByField = new LinkedHashMap<>();
        for (String field : fields) {
            Set<Type<?>> types = getTypesForField(field);
            if (types != null) {
                typesByField.put(field, types);
            }
        }
        return typesByField;
    }
    
    private Set<Type<?>> getTypesForField(String field) {
        Set<Type<?>> types = null;
        if (typesByField != null) {
            types = typesByField.get(field);
        }
        if (types == null && metadataHelper != null) {
            try {
                types = metadataHelper.getDatatypesForField(field);
            } catch (Exception e) {
                log.warn("Unable to retrieve types for field {}", field, e);
            }
        }
        return types;
    }
    
    private void generateGeoByField() {
        // convert geowave geometry ranges to geometries
        geoQueryFeatures.getGeoByField().putAll(generateGeoByField(geowaveGeometryByTierByField));
        
        // convert geowave point ranges to geometries
        geoQueryFeatures.getGeoByField().putAll(generateGeoByField(geowavePointByField, this::createGeowaveFeature));
        
        // convert geo point ranges to geometries
        geoQueryFeatures.getGeoByField().putAll(generateGeoByField(geoPointByField, this::createGeoFeature));
    }
    
    private List<String> findMatchingFunctions(String field, Geometry geometry) {
        List<String> matchingFunctions = null;
        for (GeoFunctionFeature geoFunction : geoQueryFeatures.getFunctions()) {
            if (geoFunction.getFields().contains(field) && ((Geometry) geoFunction.getGeoJson().getDefaultGeometryProperty().getValue()).intersects(geometry)) {
                if (matchingFunctions == null) {
                    matchingFunctions = new ArrayList<>();
                }
                matchingFunctions.add(geoFunction.getFunction());
            }
        }
        return matchingFunctions;
    }
    
    private Map<String,AbstractGeoTerms> generateGeoByField(Map<String,Map<Integer,List<String[]>>> geoByTierByField) {
        Map<String,AbstractGeoTerms> termsByField = new LinkedHashMap<>();
        for (Map.Entry<String,Map<Integer,List<String[]>>> rangeByTierByFieldEntry : geoByTierByField.entrySet()) {
            String fieldName = rangeByTierByFieldEntry.getKey();
            Map<Integer,GeoFeature> geoByTier = new LinkedHashMap<>();
            for (Map.Entry<Integer,List<String[]>> rangeByTierEntry : rangeByTierByFieldEntry.getValue().entrySet()) {
                geoByTier.put(rangeByTierEntry.getKey(), createGeo(fieldName, rangeByTierEntry.getValue(), this::createGeowaveFeature));
            }
            termsByField.put(fieldName, new GeoWaveGeometryTerms(geoByTier));
        }
        return termsByField;
    }
    
    private Map<String,AbstractGeoTerms> generateGeoByField(Map<String,List<String[]>> geoByField,
                    BiFunction<String,String[],SimpleFeature> fieldRangeToFeature) {
        Map<String,AbstractGeoTerms> termsByField = new LinkedHashMap<>();
        for (Map.Entry<String,List<String[]>> rangeByFieldEntry : geoByField.entrySet()) {
            String fieldName = rangeByFieldEntry.getKey();
            termsByField.put(fieldName, new GeoPointTerms(createGeo(fieldName, rangeByFieldEntry.getValue(), fieldRangeToFeature)));
        }
        return termsByField;
    }
    
    private GeoFeature createGeo(String fieldName, List<String[]> termsAndRanges, BiFunction<String,String[],SimpleFeature> fieldRangeToFeature) {
        GeoFeature geoFeature = new GeoFeature();
        if (!termsAndRanges.isEmpty()) {
            List<String> wktList = new ArrayList<>();
            List<SimpleFeature> simpleFeatures = new ArrayList<>();
            for (String[] termOrRange : termsAndRanges) {
                SimpleFeature feature = fieldRangeToFeature.apply(fieldName, termOrRange);
                simpleFeatures.add(feature);
                wktList.add((String) feature.getProperty(WKT).getValue());
            }
            geoFeature.setWkt(simpleFeatures.size() == 1 ? wktList.get(0) : "GEOMETRYCOLLECTION(" + String.join(", ", wktList) + ")");
            geoFeature.setGeoJson(DataUtilities.collection(simpleFeatures));
        }
        return geoFeature;
    }
    
    private SimpleFeature createGeoFeature(String fieldName, String[] termOrRange) {
        SimpleFeature feature = null;
        if (termOrRange.length == 2) {
            feature = createRangeFeature(fieldName, termOrRange, GeoUtils.rangeToGeometry(termOrRange[0], termOrRange[1]));
        } else if (termOrRange.length == 1) {
            feature = createTermFeature(fieldName, termOrRange[0], GeoUtils.termToGeometry(termOrRange[0]));
        }
        return feature;
    }
    
    private SimpleFeature createGeowaveFeature(String fieldName, String[] termOrRange) {
        SimpleFeature feature = null;
        if (termOrRange.length == 2) {
            feature = createRangeFeature(fieldName, termOrRange, GeoWaveUtils.rangeToGeometry(termOrRange[0], termOrRange[1]));
        } else if (termOrRange.length == 1) {
            feature = createTermFeature(fieldName, termOrRange[0], GeoWaveUtils.termToGeometry(termOrRange[0]));
        }
        return feature;
    }
    
    private SimpleFeature createFunctionFeature(String function, AbstractGeoFunctionDetails geoFunction) {
        Geometry geometry = CommonGeoUtils.geometriesToGeometry(geoFunction.getGeometry());
        featureBuilder.set(FIELDS, geoFunction.getFields().toArray(new String[0]));
        featureBuilder.set(FUNCTIONS, new String[] {function});
        featureBuilder.set(WKT, geometry.toText());
        featureBuilder.set(GEO, geometry);
        return featureBuilder.buildFeature(function);
    }
    
    private SimpleFeature createRangeFeature(String fieldName, String[] range, Geometry geometry) {
        featureBuilder.set(FIELDS, new String[] {fieldName});
        featureBuilder.set(RANGE, range);
        featureBuilder.set(WKT, geometry.toText());
        featureBuilder.set(GEO, geometry);
        List<String> matchingFunctions = findMatchingFunctions(fieldName, geometry);
        if (matchingFunctions != null) {
            featureBuilder.set(FUNCTIONS, matchingFunctions.toArray(new String[0]));
        }
        return featureBuilder.buildFeature(rangeId(fieldName, range));
    }
    
    private String rangeId(String fieldName, String[] range) {
        return fieldName + " >= '" + range[0] + "' && " + fieldName + " <= '" + range[1] + "'";
    }
    
    private SimpleFeature createTermFeature(String fieldName, String term, Geometry geometry) {
        featureBuilder.set(FIELDS, new String[] {fieldName});
        featureBuilder.set(TERM, term);
        featureBuilder.set(WKT, geometry.toText());
        featureBuilder.set(GEO, geometry);
        List<String> matchingFunctions = findMatchingFunctions(fieldName, geometry);
        if (matchingFunctions != null) {
            featureBuilder.set(FUNCTIONS, matchingFunctions.toArray(new String[0]));
        }
        return featureBuilder.buildFeature(termId(fieldName, term));
    }
    
    private String termId(String fieldName, String term) {
        return fieldName + " == '" + term + "'";
    }
}
