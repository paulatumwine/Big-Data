import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Reducer {

    Function<List<GroupByPair>, List<Pair>> reduce = l -> l.stream()
            .map(g -> new Pair(g.getKey(), g.getValues().size()))
            .collect(Collectors.toList());

    List<Pair> reduce(List<GroupByPair> groupedPairs) {
        List<Pair> reduced = new ArrayList<>();
        for (GroupByPair pair : groupedPairs) {
            reduced.add(new Pair(pair.getKey(), pair.getValues().size()));
        }
        return reduced;
    }

    static List<GroupByPair> groupByPairs(List<Pair> pairs) {
        List<GroupByPair> groupedPairs = new ArrayList<>();
        for (Pair pair : pairs) {
            Optional<GroupByPair> p = groupedPairs.stream().filter(g -> g.getKey().equals(pair.getKey())).findFirst();
            if (p.isPresent()) {
                p.get().addValue();
            } else {
                groupedPairs.add(new GroupByPair(pair.getKey()));
            }
        }
        return groupedPairs;
    }
}
