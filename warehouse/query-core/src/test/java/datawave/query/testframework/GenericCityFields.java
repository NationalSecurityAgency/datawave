package datawave.query.testframework;

import datawave.query.testframework.CitiesDataType.CityField;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Base field configuration settings for the data in the generic-cities CSV file. The default settings are:
 * <ul>
 * <li>forward index: city, state, continent</li>
 * <li>indexonly:</li>
 * <li>reverse:</li>
 * <li>multivalue fields: city, state</li>
 * <li>composite index: city|state</li>
 * <li>virtual fields: city|continent</li>
 * </ul>
 */
public class GenericCityFields extends AbstractFields {
    
    private static final Collection<String> index = Arrays.asList(CityField.CITY.name(), CityField.STATE.name(), CityField.CONTINENT.name(),
                    CityField.GEO.name());
    private static final Collection<String> indexOnly = new HashSet<>();
    private static final Collection<String> reverse = new HashSet<>();
    private static final Collection<String> multivalue = Arrays.asList(CityField.CITY.name(), CityField.STATE.name());
    
    private static final Collection<Set<String>> composite = new HashSet<>();
    private static final Collection<Set<String>> virtual = new HashSet<>();
    
    static {
        // add composite and virtual values
        Set<String> comp = new HashSet<>();
        comp.add(CityField.CITY.name());
        comp.add(CityField.STATE.name());
        composite.add(comp);
        Set<String> virt = new HashSet<>();
        virt.add(CityField.CITY.name());
        virt.add(CityField.CONTINENT.name());
        virtual.add(virt);
    }
    
    public GenericCityFields() {
        super(index, indexOnly, reverse, multivalue, composite, virtual);
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + super.toString() + "}";
    }
}
