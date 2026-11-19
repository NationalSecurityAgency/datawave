package datawave.webservice.query.result.event;

/**
 * A response object that carries the Accumulo column visibility (access expression) of the data it represents. Deployments that present security markings to
 * users translate this visibility at the response edge (for example in a {@code ResponseEnricher}).
 */
public interface HasColumnVisibility {

    /**
     * @return the column visibility expression, or null if not set
     */
    String getColumnVisibility();

    /**
     * @param columnVisibility
     *            the column visibility expression, may be null
     */
    void setColumnVisibility(String columnVisibility);
}
