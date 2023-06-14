package datawave.webservice.result;

import io.protostuff.Message;
import datawave.webservice.query.result.metadata.MetadataFieldBase;

import java.io.Serializable;
import java.util.List;

public abstract class MetadataQueryResponseBase<T> extends BaseQueryResponse implements Serializable, TotalResultsAware, Message<T> {

    public abstract List<MetadataFieldBase> getFields();

    public abstract void setTotalResults(long totalResults);

    public abstract long getTotalResults();

}
