package datawave.test.framework.util;

import java.util.ArrayList;
import java.util.List;

public class Combination {

    private Combination() {
        // enforce static access
    }

    /**
     * Generates all non-empty combinations of a given list.
     */
    public static <T> List<List<T>> getAllCombinations(List<T> list) {
        List<List<T>> result = new ArrayList<>();
        if (list == null || list.isEmpty()) {
            return result;
        }
        // Start backtracking from index 0
        backtrack(list, 0, new ArrayList<>(), result);
        return result;
    }

    private static <T> void backtrack(List<T> list, int start, List<T> current, List<List<T>> result) {
        // If the current combination has elements, add it to the result list
        if (!current.isEmpty()) {
            result.add(new ArrayList<>(current));
        }

        // Loop through the remaining elements to build combinations
        for (int i = start; i < list.size(); i++) {
            // Include the element
            current.add(list.get(i));

            // Recurse to find further combinations with the item included
            backtrack(list, i + 1, current, result);

            // Backtrack: remove the element before the next iteration
            current.remove(current.size() - 1);
        }
    }

}
