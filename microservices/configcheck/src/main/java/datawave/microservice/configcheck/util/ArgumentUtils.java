package datawave.microservice.configcheck.util;

import org.springframework.boot.ApplicationArguments;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ArgumentUtils {
    public static final String OUTPUT = "output";

    public static String getFile(ApplicationArguments args) {
        return (args.getNonOptionArgs().size() == 2) ? args.getNonOptionArgs().get(1) : null;
    }

    public static String[] getFiles(ApplicationArguments args) {
        return (args.getNonOptionArgs().size() == 3) ? new String[]{args.getNonOptionArgs().get(1), args.getNonOptionArgs().get(2)} : null;
    }

    public static List<String> getFileList(ApplicationArguments args, String option) {
        return args.getOptionValues(option).stream().flatMap(x -> Arrays.stream(x.split(","))).collect(Collectors.toList());
    }

    public static String getOutputPath(ApplicationArguments args) {
        String output = null;
        if (args.getOptionNames().contains(OUTPUT) && args.getOptionValues(OUTPUT).size() == 1) {
            output = args.getOptionValues(OUTPUT).get(0);
        }
        return output;
    }
}
