package datawave.webservice.geowave.auth.factory;

import datawave.webservice.geowave.auth.provider.DatawaveAuthorizationProvider;
import mil.nga.giat.geowave.adapter.auth.AuthorizationFactorySPI;
import mil.nga.giat.geowave.adapter.auth.AuthorizationSPI;

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
