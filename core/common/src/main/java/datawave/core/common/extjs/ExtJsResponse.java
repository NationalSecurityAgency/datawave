package datawave.core.common.extjs;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import datawave.webservice.result.BaseQueryResponse;

/**
 * Response wrapper for returning <code>T</code> objects that are compatible with ExtJS 4.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class ExtJsResponse<T> extends BaseQueryResponse {

    private static final long serialVersionUID = 1L;
    private List<T> data;
    private int total;

    @SuppressWarnings("unused")
    public ExtJsResponse() {}

    public ExtJsResponse(List<T> data) {
        this.setData(data);
    }

    public final void setData(List<T> data) {
        this.data = data;
        this.total = data.size();
        super.setHasResults(total > 0);
    }

    public List<T> getData() {
        return data;
    }

    public int getTotal() {
        return total;
    }
}
