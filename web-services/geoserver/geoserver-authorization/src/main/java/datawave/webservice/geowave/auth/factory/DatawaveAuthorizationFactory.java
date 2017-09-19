package datawave.webservice.geowave.auth.factory;

import mil.nga.giat.geowave.adapter.vector.auth.AuthorizationFactorySPI;
import mil.nga.giat.geowave.adapter.vector.auth.AuthorizationSPI;
import datawave.webservice.geowave.auth.provider.DatawaveAuthorizationProvider;

import java.net.URL;

public class DatawaveAuthorizationFactory implements AuthorizationFactorySPI {
    public AuthorizationSPI create(URL url) {
        return new DatawaveAuthorizationProvider();
    }
    
    @Override
    public String toString() {
        return "datawave";
    }
    
}
