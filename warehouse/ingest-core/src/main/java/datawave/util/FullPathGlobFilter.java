package datawave.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.PatternSyntaxException;

/**
 * Similar to GlobFilter, except FullPathGlobFilter is expected to compare the entire path in one filter. GlobFilter checks the path's name only and would
 * require a separate GlobFilter object for each of the directory names and the filename that are contained in the full path. FullPathGlobFilter also doesn't
 * accept an additional PathFilter.
 */
public class FullPathGlobFilter implements PathFilter {
    private final List<PathMatcher> pathMatchers;

    public FullPathGlobFilter(Collection<String> filePattern) throws IOException {
        pathMatchers = new ArrayList<>(filePattern.size());
        init(filePattern);
    }

    void init(Collection<String> filePatterns) throws IOException {
        try {
            for (String filePattern : filePatterns) {
                pathMatchers.add(FileSystems.getDefault().getPathMatcher("glob:" + new Path(filePattern).toUri().getPath()));
            }
        } catch (PatternSyntaxException e) {
            // Existing code expects IOException
            // startWith("Illegal file pattern")
            throw new IOException("Illegal file pattern: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean accept(Path path) {
        for (PathMatcher matcher : pathMatchers) {
            if (matcher.matches(Paths.get(path.toUri().getPath()))) {
                return true;
            }
        }
        return false;
    }
}
