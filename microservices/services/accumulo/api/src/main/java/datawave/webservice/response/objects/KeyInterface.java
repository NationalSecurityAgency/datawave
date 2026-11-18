package datawave.webservice.response.objects;

import datawave.webservice.query.result.event.HasColumnVisibility;
import datawave.webservice.query.result.event.HasMarkings;

/**
 *
 */
public interface KeyInterface extends HasMarkings, HasColumnVisibility {

    String getRow();

    String getColFam();

    String getColQual();

    Long getTimestamp();

}
