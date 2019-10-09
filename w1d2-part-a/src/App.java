import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class    App {

    public static void main(String[] args) {
        String fileName = args.length > 0 && args[0] != null ? args[0] : "testDataForW1D1.txt";

        System.out.println("Mapper Input\n\n");
        List<Pair> pairs = doMapping(fileName).orElse(null);
        System.out.println("\n\nMapper Output\n\n");
        pairs.forEach(System.out::println);

        List<GroupByPair> groupedPairs = groupByPairs(pairs);

        System.out.println("\n\nReducer Input\n\n");
        groupedPairs.forEach(System.out::println);

        List<Pair> summedPairs = reduce.apply(groupedPairs);

        System.out.println("\n\nReducer Output\n\n");
        summedPairs.forEach(System.out::println);
    }

    static Function<List<GroupByPair>, List<Pair>> reduce = l -> l.stream()
            .map(g -> new Pair(g.getKey(), g.getValues().size()))
            .collect(Collectors.toList());

    /*static List<Pair> reduce(List<GroupByPair> groupedPairs) {
        List<Pair> reduced = new ArrayList<>();
        for (GroupByPair pair: groupedPairs) {
            reduced.add(new Pair(pair.getKey(), pair.getValues().size()));
        }
        return reduced;
    }*/

    static List<GroupByPair> groupByPairs(List<Pair> pairs) {
        List<GroupByPair> groupedPairs = new ArrayList<>();
        for (Pair pair: pairs) {
            Optional<GroupByPair> p = groupedPairs.stream().filter(g -> g.getKey().equals(pair.getKey())).findFirst();
            if (p.isPresent()) {
                p.get().addValue();
            } else {
                groupedPairs.add(new GroupByPair(pair.getKey()));
            }
        }
        return groupedPairs;
    }

    static Function<Stream<String>, List<Pair>> mapper = lines -> lines
            .flatMap(l -> Arrays.stream(l.split("(\\s|-)")))
            .map(w -> w.replaceAll("(,|\")", ""))
            .filter(w -> w.matches("^[a-zA-Z]+\\.?$"))
            .map(w -> w.replaceAll("\\.", ""))
            .map(w -> new Pair(w.toLowerCase()))
            .sorted(new PairSort())
            .collect(Collectors.toList());

    static Optional<List<Pair>> doMapping(String fileName) {
        try (Stream<String> lines = Files.lines(Paths.get(fileName))) {
            Files.readAllLines(Paths.get(fileName)).forEach(System.out::println); // debug
            return Optional.of(mapper.apply(lines));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
