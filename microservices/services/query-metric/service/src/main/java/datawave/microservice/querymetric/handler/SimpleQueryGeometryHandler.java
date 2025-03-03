package datawave.microservice.querymetric.handler;

import static datawave.query.QueryParameters.QUERY_SYNTAX;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryGeometry;
import datawave.microservice.querymetric.QueryGeometryResponse;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricResponseFactory;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.GeoFeatureVisitor;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;

/**
 * This class is used to extract query geometries from the query metrics in an effort to provide those geometries for subsequent display to the user.
 */
public class SimpleQueryGeometryHandler implements QueryGeometryHandler {
    private static final Logger log = LoggerFactory.getLogger(SimpleQueryGeometryHandler.class);
    
    private static final String LUCENE = "LUCENE";
    private static final String JEXL = "JEXL";
    
    private LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
    private String basemaps;
    protected QueryMetricResponseFactory queryMetricResponseFactory;
    
    public SimpleQueryGeometryHandler(QueryMetricHandlerProperties queryMetricHandlerProperties) {
        this.basemaps = queryMetricHandlerProperties.getBaseMaps();
    }
    
    @Override
    public QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> metrics) {
        QueryGeometryResponse response = queryMetricResponseFactory.createGeoResponse();
        response.setBasemaps(basemaps);
        response.setQueryId(id);
        
        if (metrics != null) {
            Set<QueryGeometry> queryGeometries = new LinkedHashSet<>();
            for (BaseQueryMetric metric : metrics) {
                try {
                    boolean isLuceneQuery = isLuceneQuery(metric.getParameters());
                    String jexlQuery = (isLuceneQuery) ? toJexlQuery(metric.getQuery()) : metric.getQuery();
                    JexlNode queryNode = JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery);
                    Set<datawave.microservice.querymetric.QueryGeometry> features = GeoFeatureVisitor.getGeoFeatures(queryNode, isLuceneQuery);
                    queryGeometries.addAll(features.stream().map(f -> new QueryGeometry(f.getFunction(), f.getGeometry())).collect(Collectors.toList()));
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    response.addException(new Exception("Unable to parse the geo features"));
                }
            }
            response.setResult(new ArrayList<>(queryGeometries));
        }
        
        return response;
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
            
            return !GeoFeatureVisitor.getGeoFeatures(JexlASTHelper.parseAndFlattenJexlQuery(jexlQuery)).isEmpty();
        } catch (Exception e) {
            log.trace("Unable to parse the geo features", e);
        }
        return false;
    }
    
    @Override
    public void setQueryMetricResponseFactory(QueryMetricResponseFactory queryMetricResponseFactory) {
        this.queryMetricResponseFactory = queryMetricResponseFactory;
    }
}
