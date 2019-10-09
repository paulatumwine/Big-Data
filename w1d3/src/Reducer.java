import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Reducer {

    Function<List<GroupByPair>, List<Pair>> reduce = l -> l.stream()
            .map(g -> new Pair(g.getKey(), ((List) g.getValues()).size()))
            .collect(Collectors.toList());

    static List<GroupByPair> groupByPairs(List<Pair> pairs) {
        List<GroupByPair> groupedPairs = new ArrayList<>();
        for (Pair pair : pairs) {
            Optional<GroupByPair> p = groupedPairs.stream().filter(g -> g.getKey().equals(pair.getKey())).findFirst();
            if (p.isPresent()) {
                List values = (List<Integer>) p.get().getValues();

                List<List<Integer>> tmp = new ArrayList<>();
                tmp.addAll(values);
                tmp.add((List<Integer>) pair.getValue());

                groupedPairs.set(groupedPairs.indexOf(p.get()), new GroupByPair(pair.getKey(), tmp));
            } else {
                List<List<Integer>> tmp = new ArrayList<>();
                tmp.add((List<Integer>) pair.getValue());
                groupedPairs.add(new GroupByPair(pair.getKey(), tmp));
            }
        }
        return groupedPairs;
    }

    List<Pair> reduce(List<GroupByPair> groupedPairs) {
        List<Pair> reduced = new ArrayList<>();
        for (GroupByPair pair : groupedPairs) {
            reduced.add(new Pair(pair.getKey(), computeAverage((List<List<Integer>>) pair.getValues())));
        }
        return reduced.stream().sorted(new PairSort()).collect(Collectors.toList());
    }

    double computeAverage(List<List<Integer>> lists) {
        double sum = 0;
        double count = 0;
        for (List<Integer> list : lists) {
            sum += list.get(0);
            count += list.get(1);
        }
        return sum / count;
    }
}
