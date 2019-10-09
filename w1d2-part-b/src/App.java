import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class App {

    public static void main(String[] args) {
        String fileName = args.length > 0 && args[0] != null ? args[0] : "testDataForW1D1.txt";
        Integer inputSplits = args.length > 1 && args[1] != null ? Integer.parseInt(args[1]) : 3;
        int reducers = args.length > 2 && args[2] != null ? Integer.parseInt(args[2]) : 4;

        System.out.println("Number of Input-Splits: " + inputSplits);
        System.out.println("Number of Reducers: " + reducers);

        WordCount wordCount = new WordCount(inputSplits, reducers);

        try {
            List<String> lines = Files.readAllLines(Paths.get(fileName))
                    .stream().collect(Collectors.toList());

            List<List<String>> input = getSplits(lines, inputSplits);
            for (int i = 0; i < inputSplits; i++) {
                System.out.println("Mapper " + i + " Input");
                input.get(i).forEach(System.out::println);
            }
            List<List<Pair>> output = new ArrayList<>();
            for (int i = 0; i < inputSplits; i++) {
                output.add(wordCount.getM().get(i).map(input.get(i)));
                System.out.println("Mapper " + i + " Output");
                output.get(i).forEach(System.out::println);
            }

            List<Map<Integer, List<Pair>>> rs = new ArrayList<>();
            for (int i = 0; i < inputSplits; i++) {
                rs.add(output.get(i).stream().collect(Collectors.groupingBy(pair -> wordCount.getPartition(pair.getKey(), reducers))));
            }
            // System.out.println(rs);
            for (int i = 0; i < inputSplits; i++) {
                for (Object k : rs.get(i).keySet()) {
                    Integer key = (Integer) k;
                    System.out.println("Pairs sent from Mapper " + i + " to Reducer " + key);
                    rs.get(i).get(key).forEach(System.out::println);
                }
            }

            Map<Integer, List<Pair>> rearranged = new HashMap<>();
            for (int i = 0; i < reducers; i++) {
                int finalI = i;
                rs.forEach(m -> {
                    List<Pair> tmp = m.get(finalI);
                    if (rearranged.containsKey(finalI)) {
                        rearranged.get(finalI).addAll(tmp);
                    } else {
                        rearranged.put(finalI, tmp);
                    }
                });
            }

            Map<Integer, List<GroupByPair>> reducerInput = new HashMap<>();
            for (int i = 0; i < reducers; i++) {
                int finalI = i;
                rs.forEach(m -> {
                    reducerInput.put(finalI, Reducer.groupByPairs(rearranged.get(finalI)));
                });
            }

            for (int i = 0; i < reducers; i++) {
                System.out.println("Reducer " + i + " input");
                reducerInput.get(i).forEach(System.out::println);
            }

            List<List<Pair>> reducerOutput = new ArrayList<>();
            for (int i = 0; i < reducers; i++) {
                reducerOutput.add(wordCount.getR().get(i).reduce.apply(reducerInput.get(i)));
            }
            for (int i = 0; i < reducers; i++) {
                System.out.println("Reducer " + i + " output");
                reducerOutput.get(i).forEach(System.out::println);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static List<List<String>> getSplits(List<String> words, int inputSplits) {
        List<List<String>> splits = new ArrayList<>();
        int splitSize = (int) Math.floor(words.size() / inputSplits);
        for (int i = 0; i < words.size(); i += splitSize)
            splits.add(words.subList(i, Math.min(splitSize + i, words.size())));
        return splits;
    }
}
