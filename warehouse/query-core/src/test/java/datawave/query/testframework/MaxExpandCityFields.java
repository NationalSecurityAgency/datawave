package datawave.query.testframework;

import datawave.query.testframework.CitiesDataType.CityField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Base field configuration settings for the data in the generic-cities CSV file
 */
public class MaxExpandCityFields extends AbstractFields {
    
    private static final Collection<String> index = new ArrayList<>(Arrays.asList(CityField.CITY.name(), CityField.STATE.name(), CityField.CODE.name()));
    private static final Collection<String> indexOnly = new HashSet<>();
    private static final Collection<String> reverse = new HashSet<>();
    private static final Collection<String> multivalue = Arrays.asList(CityField.CITY.name(), CityField.STATE.name());
    
    private static final Collection<Set<String>> composite = new HashSet<>();
    private static final Collection<Set<String>> virtual = new HashSet<>();
    
    public MaxExpandCityFields() {
        super(index, indexOnly, reverse, multivalue, composite, virtual);
    }
    
    @Override
    public String toString() {
        return "MaxExpansionCityFields{" + super.toString() + "}";
    }
}
