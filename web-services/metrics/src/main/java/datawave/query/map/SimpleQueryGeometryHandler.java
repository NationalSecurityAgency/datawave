package datawave.query.map;

import static datawave.query.QueryParameters.QUERY_SYNTAX;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.language.parser.ParseException;
import datawave.core.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.core.query.map.QueryGeometryHandler;
import datawave.microservice.map.data.GeoFunctionFeature;
import datawave.microservice.map.data.GeoQueryFeatures;
import datawave.microservice.map.geojson.GeoJSON;
import datawave.microservice.map.visitor.GeoFeatureVisitor;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryGeometry;
import datawave.microservice.querymetric.QueryGeometryResponse;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.metrics.ShardTableQueryMetricHandler;

/**
 * This class is used to extract query geometries from the query metrics in an effort to provide those geometries for subsequent display to the user.
 */
@ApplicationScoped
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class SimpleQueryGeometryHandler implements QueryGeometryHandler {
    private static final Logger log = ThreadConfigurableLogger.getLogger(ShardTableQueryMetricHandler.class);

    private static final String LUCENE = "LUCENE";
    private static final String JEXL = "JEXL";

    private LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();

    @Inject
    @ConfigProperty(name = "dw.basemaps", defaultValue = "{}")
    private String basemaps;

    @Override
    public QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> metrics) {
        QueryGeometryResponse response = new QueryGeometryResponse();
        response.setQueryId(id);
        response.setBasemaps(basemaps);
        if (metrics != null) {
            Set<QueryGeometry> queryGeometries = new LinkedHashSet<>();
            for (BaseQueryMetric metric : metrics) {
                try {
                    boolean isLuceneQuery = isLuceneQuery(metric.getParameters());
                    String jexlQuery = (isLuceneQuery) ? toJexlQuery(metric.getQuery()) : metric.getQuery();
                    JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery);
                    queryGeometries.addAll(geoQueryFeaturesToQueryGeometry(GeoFeatureVisitor.getGeoFeatures(queryNode, Collections.emptyMap())));
                } catch (Exception e) {
                    response.addException(new Exception("Unable to parse the geo features"));
                }
            }
            response.setResult(new ArrayList<>(queryGeometries));
        }

        return response;
    }

    private List<QueryGeometry> geoQueryFeaturesToQueryGeometry(GeoQueryFeatures geoQueryFeatures) {
        List<QueryGeometry> queryGeometries = new ArrayList<>();
        for (GeoFunctionFeature geoFuncFeature : geoQueryFeatures.getFunctions()) {

            String function = geoFuncFeature.getFunction();
            String geometry = null;
            try {
                StringWriter writer = new StringWriter();
                GeoJSON.write(geoFuncFeature.getGeoJson(), writer);
                geometry = writer.toString();
            } catch (IOException e) {
                log.trace("Unable to serialize the geo features");
                continue;
            }
            queryGeometries.add(new QueryGeometry(function, geometry));
        }
        return queryGeometries;
    }

    private static boolean isLuceneQuery(Set<QueryImpl.Parameter> parameters) {
        return parameters.stream().anyMatch(p -> p.getParameterName().equals(QUERY_SYNTAX) && p.getParameterValue().equals(LUCENE));
    }

    private String toJexlQuery(String query) throws ParseException {
        return toJexlQuery(query, parser);
    }

    private static String toJexlQuery(String query, LuceneToJexlQueryParser parser) throws ParseException {
        return parser.parse(query).getOriginalQuery();
    }

    public static boolean isGeoQuery(BaseQueryMetric metric) {
        try {
            String jexlQuery = metric.getQuery();
            if (isLuceneQuery(metric.getParameters()))
                jexlQuery = toJexlQuery(jexlQuery, new LuceneToJexlQueryParser());

            return !GeoFeatureVisitor.getGeoFeatures(JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery), Collections.emptyMap()).getFunctions().isEmpty();
        } catch (Exception e) {
            log.trace(new Exception("Unable to parse the geo features"));
        }
        return false;
    }
}
