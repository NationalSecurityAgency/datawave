package datawave.microservice.query.mapreduce;

import java.io.File;

import org.springframework.boot.loader.tools.Layout;
import org.springframework.boot.loader.tools.LayoutFactory;
import org.springframework.boot.loader.tools.Layouts;
import org.springframework.boot.loader.tools.LibraryScope;

public class MapReduceLayoutFactory implements LayoutFactory {
    @Override
    public Layout getLayout(File source) {
        return new Layouts.Jar() {
            @Override
            public String getLibraryLocation(String libraryName, LibraryScope scope) {
                return "lib/";
            }
            
            @Override
            public String getClassesLocation() {
                return "classes/";
            }
            
            @Override
            public String getRepackagedClassesLocation() {
                return "classes/";
            }
        };
    }
}
