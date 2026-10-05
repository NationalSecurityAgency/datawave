package datawave.webservice.query.result.event;

import datawave.marking.Markings;

public interface HasMarkings {

    void setMarkings(Markings<?> markings);

    Markings<?> getMarkings();
}
