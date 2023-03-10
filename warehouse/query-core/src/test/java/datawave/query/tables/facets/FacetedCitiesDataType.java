package datawave.query.tables.facets;

import datawave.ingest.data.TypeRegistry;
import datawave.ingest.mapreduce.handler.facet.FacetHandler;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.query.QueryTestTableHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.FieldConfig;
import datawave.webservice.query.Query;

import java.io.IOException;
import java.net.URISyntaxException;

public class FacetedCitiesDataType extends CitiesDataType {
    
    public FacetedCitiesDataType(final CityEntry city, final FieldConfig config) throws IOException, URISyntaxException {
        this(city.name(), city.getIngestFile(), config);
    }
    
    public FacetedCitiesDataType(final String city, final String ingestFile, final FieldConfig config) throws IOException, URISyntaxException {
        super(city, ingestFile, config);
        this.hConf.set(this.dataType + TypeRegistry.HANDLER_CLASSES, String.join(",", AbstractColumnBasedHandler.class.getName(), FacetHandler.class.getName()));
        this.hConf.set(this.dataType + ".facet.category.name" + ".continent", "CONTINENT;STATE,CITY");
        this.hConf.set(this.dataType + ".facet.category.name" + ".code", "CODE;STATE,CITY");
        
        this.hConf.set(FacetHandler.FACET_TABLE_NAME, QueryTestTableHelper.FACET_TABLE_NAME);
        this.hConf.set(FacetHandler.FACET_METADATA_TABLE_NAME, QueryTestTableHelper.FACET_METADATA_TABLE_NAME);
        this.hConf.set(FacetHandler.FACET_HASH_TABLE_NAME, QueryTestTableHelper.FACET_HASH_TABLE_NAME);
    }
}
