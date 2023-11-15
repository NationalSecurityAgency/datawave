package datawave.util.flag;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

/**
 * Defines sets of InputFile instances to assist with tests
 */
public class InputFileSets {
    static final HashSet<InputFile> EMPTY_FILES = new HashSet<>();
    static final HashSet<InputFile> SINGLE_FILE = Sets.newHashSet(createInputFile("f"));
    static final HashSet<InputFile> MULTIPLE_FILES = createManyInputFiles(20);
    static final HashSet<InputFile> MANY_FILES = createManyInputFiles(10 * 1000);
    static final List<HashSet<InputFile>> VARIOUS_SETS_OF_FILES = Arrays.asList(EMPTY_FILES, SINGLE_FILE, MULTIPLE_FILES, MANY_FILES);

    static HashSet<InputFile> createManyInputFiles(int numberToCreate) {
        HashSet<InputFile> result = Sets.newHashSetWithExpectedSize(numberToCreate);
        char name = 'a';
        for (int i = 0; i < numberToCreate; i++) {
            result.add(createInputFile(String.valueOf(name++)));
        }
        assert result.size() == numberToCreate;
        return result;
    }

    static InputFile createInputFile(String pathStr) {
        return new InputFile("d", new Path(pathStr), 0, 0, 0, "b");
    }

    static HashSet<InputFile> withNewBaseDir(Set<InputFile> inputSet, String baseDir) {
        HashSet<InputFile> result = Sets.newHashSetWithExpectedSize(inputSet.size());
        for (InputFile inputFile : inputSet) {
            InputFile replacementInputFile = new InputFile(inputFile.getFolder(), inputFile.getPath(), inputFile.getBlocksize(), inputFile.getFilesize(),
                            inputFile.getTimestamp(), baseDir);
            result.add(replacementInputFile);
        }
        return result;
    }
}
