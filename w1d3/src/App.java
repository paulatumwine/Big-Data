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
        Integer inputSplits = args.length > 1 && args[1] != null ? Integer.parseInt(args[1]) : 4;
        int reducers = args.length > 2 && args[2] != null ? Integer.parseInt(args[2]) : 3;

        System.out.println("Number of Input-Splits: " + inputSplits);
        System.out.println("Number of Reducers: " + reducers);

        InMapperWordCount wordCount = new InMapperWordCount(inputSplits, reducers);

        try {
            List<String> lines = Files.readAllLines(Paths.get(fileName))
                    .stream().collect(Collectors.toList());

            List<List<String>> mapperInputs = getSplits(lines, inputSplits);

            // run the mappers
            for (int i = 0; i < inputSplits; i++) {
                System.out.println("Mapper " + i + " Input");
                mapperInputs.get(i).forEach(System.out::println);
            }
            List<List<Pair<Character, List<Integer>>>> mapperOutputs = new ArrayList<>();
            for (int i = 0; i < inputSplits; i++) {
                mapperOutputs.add(wordCount.getM().get(i).map(mapperInputs.get(i)).emit());
                System.out.println("Mapper " + i + " Output");
                mapperOutputs.get(i).forEach(pair -> System.out.println("< " + pair.getKey() + ", " + pair.getValue()));
            }

            // shuffle - starting with the mapper to reducer mappings
            List<Map<Integer, List<Pair>>> mappings = new ArrayList<>();
            for (int i = 0; i < inputSplits; i++) {
                mappings.add(mapperOutputs.get(i).stream().collect(Collectors.groupingBy(pair -> wordCount.getPartition(pair.getKey().toString(), reducers))));
            }
            // System.out.println(mappings);
            for (int i = 0; i < inputSplits; i++) {
                for (Object k : mappings.get(i).keySet()) {
                    Integer key = (Integer) k;
                    System.out.println("Pairs sent from Mapper " + i + " to Reducer " + key);
                    mappings.get(i).get(key).forEach(System.out::println);
                }
            }
            // create reducer inputs
            Map<Integer, List<Pair>> rearranged = new HashMap<>();
            for (int i = 0; i < reducers; i++) {
                int finalI = i;
                mappings.forEach(m -> {
                    List<Pair> tmp = m.get(finalI);
                    if (rearranged.containsKey(finalI)) {
                        rearranged.get(finalI).addAll(tmp);
                    } else {
                        rearranged.put(finalI, tmp);
                    }
                });
            }
            Map<Integer, List<GroupByPair>> reducerInputs = new HashMap<>();
            for (int i = 0; i < reducers; i++) {
                int finalI = i;
                mappings.forEach(m -> {
                    reducerInputs.put(finalI, Reducer.groupByPairs(rearranged.get(finalI)));
                });
            }

            // run the reducers
            for (int i = 0; i < reducers; i++) {
                System.out.println("Reducer " + i + " input");
                reducerInputs.get(i).forEach(System.out::println);
            }

            List<List<Pair>> reducerOutputs = new ArrayList<>();
            for (int i = 0; i < reducers; i++) {
                reducerOutputs.add(wordCount.getR().get(i).reduce(reducerInputs.get(i)));
            }
            for (int i = 0; i < reducers; i++) {
                System.out.println("Reducer " + i + " output");
                reducerOutputs.get(i).forEach(System.out::println);
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
