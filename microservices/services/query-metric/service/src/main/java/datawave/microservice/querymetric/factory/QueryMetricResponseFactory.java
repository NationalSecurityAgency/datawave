package datawave.microservice.querymetric.factory;

import datawave.microservice.http.converter.html.BannerProvider;
import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.microservice.querymetric.QueryGeometryResponse;
import datawave.microservice.querymetric.QueryMetricListResponse;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;

public class QueryMetricResponseFactory {
    
    protected BannerProvider bannerProvider;
    protected String basePath;
    
    public QueryMetricResponseFactory(BannerProvider bannerProvider, String basePath) {
        this.bannerProvider = bannerProvider;
        this.basePath = basePath;
    }
    
    public BaseQueryMetricListResponse createListResponse() {
        QueryMetricListResponse response = new QueryMetricListResponse();
        if (bannerProvider != null) {
            response.setHeader(bannerProvider.getHeadBanner());
            response.setFooter(bannerProvider.getFootBanner());
        }
        response.setBasePath(basePath);
        return response;
    }
    
    public QueryMetricsSummaryResponse createSummaryResponse() {
        QueryMetricsSummaryResponse response = new QueryMetricsSummaryResponse();
        if (bannerProvider != null) {
            response.setHeader(bannerProvider.getHeadBanner());
            response.setFooter(bannerProvider.getFootBanner());
        }
        response.setBasePath(basePath);
        return response;
    }
    
    public QueryGeometryResponse createGeoResponse() {
        QueryGeometryResponse response = new QueryGeometryResponse();
        if (bannerProvider != null) {
            response.setHeader(bannerProvider.getHeadBanner());
            response.setFooter(bannerProvider.getFootBanner());
        }
        response.setBasePath(basePath);
        return response;
    }
    
}
