package datawave.webservice.result;

import java.util.List;

import datawave.webservice.query.result.event.EventBase;

public abstract class EventQueryResponseBase extends BaseQueryResponse {

    private static final long serialVersionUID = -8404343960387980525L;

    public abstract Long getTotalEvents();

    public abstract long getTotalResults();

    public abstract Long getReturnedEvents();

    public abstract List<String> getFields();

    public abstract List<EventBase> getEvents();

    public abstract void setFields(List<String> fields);

    public abstract void setEvents(List<EventBase> entries);

    public abstract void setTotalEvents(Long events);

    public abstract void setTotalResults(long events);

    public abstract void setReturnedEvents(Long size);

    public abstract void merge(EventQueryResponseBase response);

}
