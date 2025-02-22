package datawave.microservice.querymetric;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMetricModel extends QueryMetric implements QueryMetricModelFormat {
    
    private static final Logger log = LoggerFactory.getLogger(QueryMetricModel.class);
    public long totalPageTime = 0;
    public long totalPageCallTime = 0;
    public long totalSerializationTime = 0;
    public long totalBytesSent = 0;
    
    private String basePath;
    private NumberFormat nf = NumberFormat.getIntegerInstance();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
    
    public QueryMetricModel(QueryMetric queryMetric, String basePath) {
        super(queryMetric);
        this.basePath = basePath;
    }
    
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }
    
    public String getCreateDateStr() {
        try {
            return createDate == null ? "" : sdf.format(createDate);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "";
        }
    }
    
    @Override
    public String getUser() {
        return user == null ? "" : user;
    }
    
    public String getBeginDateStr() {
        try {
            return beginDate == null ? "" : sdf.format(beginDate);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "";
        }
    }
    
    public String getEndDateStr() {
        try {
            return endDate == null ? "" : sdf.format(endDate);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "";
        }
    }
    
    public String getQueryEscapedForTemplateString() {
        return escapeForJavascriptTemplateString(super.getQuery());
    }
    
    public String getPlanEscapedForTemplateString() {
        return escapeForJavascriptTemplateString(super.getPlan());
    }
    
    public String getQueryIdUrl() {
        return basePath + "/v1/id/" + queryId;
    }
    
    public String getProxyServersStr() {
        return getProxyServers() == null ? "" : StringUtils.join(getProxyServers(), "<BR/>");
    }
    
    public String getQueryStyle() {
        return isJexlQuery(parameters) ? "white-space:pre-wrap; overflow-wrap:anywhere;" : "overflow-wrap:anywhere;";
    }
    
    public String getParametersStr() {
        return parameters == null ? "" : toFormattedParametersString(parameters);
    }
    
    public String getQueryAuthorizationsStr() {
        String queryAuthorizations = getQueryAuthorizations();
        if (queryAuthorizations == null) {
            return "";
        } else {
            return Arrays.stream(queryAuthorizations.split(",")).sorted().collect(Collectors.joining(" "));
        }
    }
    
    public String getPredictionsStr() {
        return getPredictionsStr(predictions);
    }
    
    public String getLoginTimeStr() {
        return numToString(getLoginTime(), 0);
    }
    
    public String getSetupTimeStr() {
        return numToString(getSetupTime(), 0);
    }
    
    public String getCreateCallTimeStr() {
        return numToString(getCreateCallTime(), 0);
    }
    
    public String getNumPagesStr() {
        return nf.format(numPages);
    }
    
    public String getNumResultsStr() {
        return nf.format(numResults);
    }
    
    public String getDocSizeStr() {
        return nf.format(docSize);
    }
    
    public String getDocRangesStr() {
        return nf.format(docRanges);
    }
    
    public String getFiRangesStr() {
        return nf.format(fiRanges);
    }
    
    public String getSourceCountStr() {
        return nf.format(sourceCount);
    }
    
    public String getNextCountStr() {
        return nf.format(nextCount);
    }
    
    public String getSeekCountStr() {
        return nf.format(seekCount);
    }
    
    public String getYieldCountStr() {
        return nf.format(yieldCount);
    }
    
    public String getVersionStr() {
        return versionMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("<br/>"));
    }
    
    public String getTotalPageTimeStr() {
        return nf.format(totalPageTime);
    }
    
    public String getTotalPageCallTimeStr() {
        return nf.format(totalPageCallTime);
    }
    
    public String getTotalSerializationTimeStr() {
        return nf.format(totalSerializationTime);
    }
    
    public String getTotalBytesSentStr() {
        return nf.format(totalBytesSent);
    }
    
    public String getElapsedTimeStr() {
        return nf.format(getElapsedTime());
    }
    
    @Override
    public String getErrorCode() {
        return errorCode == null ? "" : StringEscapeUtils.escapeHtml4(errorCode);
    }
    
    @Override
    public String getErrorMessage() {
        return errorMessage == null ? "" : StringEscapeUtils.escapeHtml4(errorMessage);
    }
    
    public List<PageMetricModel> getPageTimeModels() {
        return super.getPageTimes().stream().map(p -> new PageMetricModel(p)).collect(Collectors.toList());
    }
}
