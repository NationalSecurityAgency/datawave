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
public class MultiValueCityFields extends AbstractFields {
    
    private static final Collection<String> index = new ArrayList<>(Arrays.asList(CityField.CITY.name(), CityField.STATE.name(), CityField.CONTINENT.name()));
    private static final Collection<String> indexOnly = new HashSet<>();
    private static final Collection<String> reverse = new HashSet<>();
    private static final Collection<String> multivalue = Arrays.asList(CityField.CITY.name(), CityField.STATE.name());
    
    private static final Collection<Set<String>> composite = new HashSet<>();
    private static final Collection<Set<String>> virtual = new HashSet<>();
    
    static {
        Set<String> comp = new HashSet<>();
        comp.add(CityField.CITY.name());
        comp.add(CityField.STATE.name());
        composite.add(comp);
        Set<String> virt = new HashSet<>();
        virt.add(CityField.CITY.name());
        virt.add(CityField.CONTINENT.name());
        virtual.add(virt);
    }
    
    public MultiValueCityFields() {
        super(index, indexOnly, reverse, multivalue, composite, virtual);
    }
    
    @Override
    public String toString() {
        return "MultiValueCityFields{" + super.toString() + "}";
    }
}
