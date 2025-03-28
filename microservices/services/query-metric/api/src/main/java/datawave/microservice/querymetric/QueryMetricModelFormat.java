package datawave.microservice.querymetric;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import datawave.microservice.query.QueryImpl;

public interface QueryMetricModelFormat {
    
    char DOLLAR = '$';
    char BACKSLASH = '\\';
    char BACKTICK = '`';
    
    NumberFormat nf = NumberFormat.getIntegerInstance();
    
    void setBasePath(String basePath);
    
    String getCreateDateStr();
    
    String getBeginDateStr();
    
    String getEndDateStr();
    
    String getQueryIdUrl();
    
    String getProxyServersStr();
    
    String getParametersStr();
    
    String getQueryAuthorizationsStr();
    
    String getPredictionsStr();
    
    String getLoginTimeStr();
    
    String getSetupTimeStr();
    
    String getCreateCallTimeStr();
    
    String getNumPagesStr();
    
    String getNumResultsStr();
    
    String getDocSizeStr();
    
    String getDocRangesStr();
    
    String getFiRangesStr();
    
    String getSourceCountStr();
    
    String getNextCountStr();
    
    String getSeekCountStr();
    
    String getYieldCountStr();
    
    String getVersionStr();
    
    String getTotalPageTimeStr();
    
    String getTotalPageCallTimeStr();
    
    String getTotalSerializationTimeStr();
    
    String getTotalBytesSentStr();
    
    String getElapsedTimeStr();
    
    List<PageMetricModel> getPageTimeModels();
    
    default String getPredictionsStr(Set<BaseQueryMetric.Prediction> predictions) {
        StringBuilder builder = new StringBuilder();
        if (predictions != null && !predictions.isEmpty()) {
            String delimiter = "";
            List<BaseQueryMetric.Prediction> predictionsList = new ArrayList<>(predictions);
            Collections.sort(predictionsList);
            for (BaseQueryMetric.Prediction p : predictionsList) {
                builder.append(delimiter).append(p.getName()).append(" = ").append(p.getPrediction());
                delimiter = "<br>";
            }
        } else {
            builder.append("");
        }
        return builder.toString();
    }
    
    default boolean isJexlQuery(Set<QueryImpl.Parameter> params) {
        return params.stream().anyMatch(p -> p.getParameterName().equals("query.syntax") && p.getParameterValue().equals("JEXL"));
    }
    
    default String numToString(long number, long minValue) {
        return number < minValue ? "" : nf.format(number);
    }
    
    default String toFormattedParametersString(final Set<QueryImpl.Parameter> parameters) {
        final StringBuilder params = new StringBuilder();
        final String PARAMETER_SEPARATOR = "<BR/>";
        final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
        
        if (null != parameters) {
            for (final QueryImpl.Parameter param : parameters) {
                if (params.length() > 0) {
                    params.append(PARAMETER_SEPARATOR);
                }
                
                params.append(param.getParameterName());
                params.append(PARAMETER_NAME_VALUE_SEPARATOR);
                params.append(param.getParameterValue());
            }
        }
        return params.toString();
    }
    
    default String escapeForJavascriptTemplateString(String str) {
        if (StringUtils.isBlank(str)
                        || (!str.contains(String.valueOf(DOLLAR)) && !str.contains(String.valueOf(BACKSLASH)) && !str.contains(String.valueOf(BACKTICK)))) {
            return str;
        } else {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < str.length(); i++) {
                boolean lastChar = (i == str.length() - 1);
                char c = str.charAt(i);
                switch (c) {
                    case BACKTICK: // must escape ` to prevent from breaking JS template string
                    case BACKSLASH: // must escape \ to prevent it from vanishing when preceding a non-escape character
                        sb.append("\\");
                        break;
                    case DOLLAR: // must escape $ to prevent JS template string interpretation as placeholder
                        if (!lastChar && str.charAt(i + 1) == '{') {
                            sb.append("\\");
                        }
                        break;
                    default:
                }
                // append the original character
                sb.append(c);
            }
            return sb.toString();
        }
    }
}
