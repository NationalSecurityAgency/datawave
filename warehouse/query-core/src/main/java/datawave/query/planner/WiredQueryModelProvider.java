package datawave.query.planner;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import datawave.query.model.QueryModel;

/**
 * Intended to be dependency-injected
 */
public class WiredQueryModelProvider implements QueryModelProvider {

    protected QueryModel queryModel;

    @Override
    public QueryModel getQueryModel() {
        return queryModel;
    }

    // public void setQueryModel(QueryModel queryModel) {
    // this.queryModel = queryModel;
    // }

    public void setQueryModel(Map<String,String> mappings) {
        QueryModel model = new QueryModel();
        for (Map.Entry<String,String> mapping : mappings.entrySet()) {
            for (String value : StringUtils.split(mapping.getValue(), ',')) {
                model.addTermToModel(mapping.getKey(), value);
                model.addTermToReverseModel(value, mapping.getKey());
            }
        }
        this.queryModel = model;
    }
}
