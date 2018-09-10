package datawave.query.testframework.cardata;

import datawave.query.testframework.cardata.CarsDataType.CarField;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class GenericCarFields extends AbstractCarFields {
    
    private static final Collection<String> index = Arrays.asList(CarField.MAKE.name(), CarField.MODEL.name(), CarField.WHEELS.name(), CarField.COLOR.name());
    private static final Collection<String> indexOnly = new HashSet<>();
    private static final Collection<String> reverse = new HashSet<>();
    private static final Collection<String> multivalue = Arrays.asList(CarField.MAKE.name(), CarField.MODEL.name(), CarField.WHEELS.name(),
                    CarField.COLOR.name());
    
    private static final Collection<Set<String>> composite = new HashSet<>();
    private static final Collection<Set<String>> virtual = new HashSet<>();
    
    static {
        // add composite and virtual values
        Set<String> comp = new HashSet<>();
        comp.add(CarField.MAKE.name());
        comp.add(CarField.MODEL.name());
        comp.add(CarField.COLOR.name());
        // composite does not work yet
        // composite.add(comp);
        Set<String> virt = new HashSet<>();
        // Do nothing for virtual fields, for now.
        // virt.add(CitiesDataType.CityField.CITY.name());
        // virt.add(CitiesDataType.CityField.CONTINENT.name());
        virtual.add(virt);
    }
    
    public GenericCarFields() {
        super(index, indexOnly, reverse, multivalue, composite, virtual);
    }
    
    @Override
    public String toString() {
        return "GenericCarFields{" + super.toString() + "}";
    }
}
