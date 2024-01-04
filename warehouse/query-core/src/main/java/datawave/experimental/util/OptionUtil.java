package datawave.experimental.util;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/**
 * Simple util for serializing and deserializing iterator options
 */
public class OptionUtil {

    public String listToString(List<String> list) {
        return Joiner.on(',').join(list);
    }

    public List<String> listFromString(String s) {
        return Splitter.on(',').splitToList(s);
    }
}
