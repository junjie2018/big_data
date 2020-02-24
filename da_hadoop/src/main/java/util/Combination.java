package util;

import java.util.*;

/*
    解决的是一个算法问题，给你一个集合，如：
        {1,2,3}
    返回该结合的所有组合：
        {},{1},{2},{3},{1,2},{1,3},{2,3},{1,2,3}
 */
public class Combination {
    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {
        List<List<T>> result = new ArrayList<>();
        for (int i = 0; i <= elements.size(); i++) {
            result.addAll(findSortedCombinations(elements, i));
        }
        return result;
    }

    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<>();

        if (n == 0) {
            result.add(new ArrayList<>());
            return result;
        }

        List<List<T>> combinations = findSortedCombinations(elements, n - 1);
        for (List<T> combination : combinations) {
            for (T element : elements) {
                if (combination.contains(element)) {
                    continue;
                }

                List<T> list = new ArrayList<T>();
                list.addAll(combination);

                if (list.contains(element)) {
                    continue;
                }

                list.add(element);
                //sort items not to duplicate the items
                //   example: (a, b, c) and (a, c, b) might become
                //   different items to be counted if not sorted
                Collections.sort(list);

                if (result.contains(list)) {
                    continue;
                }

                result.add(list);
            }
        }

        return result;
    }

    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinationsFast(List<T> elements) {
        // todo 有时间实现
        return null;
    }

    protected static <T extends Comparable<? super T>> List<List<T>> findSortedCombinationsFast(List<T> elements, int idx) {
        // todo 有时间实现
        return null;
    }

    public static void main(String[] args) {
        List<String> elements = Arrays.asList("a", "b", "c", "d", "e");
        List<List<String>> combinations = findSortedCombinations(elements, 2);
        System.out.println(combinations);
    }
}
