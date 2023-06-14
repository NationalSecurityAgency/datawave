package datawave.query.transformer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;

import org.apache.commons.collections4.Transformer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class EventQueryDataDecoratorTransformer implements Transformer {

    private Map<String,EventQueryDataDecorator> dataDecorators = new LinkedHashMap<>();
    private List<String> requestedDecorators = new ArrayList<>();

    public EventQueryDataDecoratorTransformer() {

    }

    public EventQueryDataDecoratorTransformer(EventQueryDataDecoratorTransformer other) {
        this.dataDecorators = other.dataDecorators;
        this.requestedDecorators = other.requestedDecorators;
    }

    @Override
    public Object transform(Object o) {

        if (o instanceof EventBase) {
            EventBase e = (EventBase) o;
            List<? extends FieldBase> fields = e.getFields();
            Multimap<String,FieldBase> fieldMap = HashMultimap.create();
            for (FieldBase f : fields) {
                fieldMap.put(f.getName(), f);
            }

            for (String d : requestedDecorators) {

                EventQueryDataDecorator decorator = dataDecorators.get(d);
                if (decorator != null) {
                    decorator.decorateData(fieldMap);
                }
            }
            e.setFields(new ArrayList<>(fieldMap.values()));
            return e;
        } else {
            return o;
        }
    }

    public Map<String,EventQueryDataDecorator> getDataDecorators() {
        return dataDecorators;
    }

    public void setDataDecorators(Map<String,EventQueryDataDecorator> dataDecorators) {
        this.dataDecorators = dataDecorators;
    }

    public List<String> getRequestedDecorators() {
        return requestedDecorators;
    }

    public void setRequestedDecorators(List<String> requestedDecorators) {
        this.requestedDecorators = requestedDecorators;

    }

}
