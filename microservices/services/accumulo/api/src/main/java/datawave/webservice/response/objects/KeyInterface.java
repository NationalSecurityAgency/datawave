package datawave.webservice.response.objects;

import datawave.webservice.query.result.event.HasColumnVisibility;

/**
 *
 */
public interface KeyInterface extends HasColumnVisibility {

    String getRow();

    String getColFam();

    String getColQual();

    Long getTimestamp();

}
