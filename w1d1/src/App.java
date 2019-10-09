import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class App {

    static List<Pair> words = new ArrayList<>();

    public static void main(String[] args) {
        /*System.out.println(new File("").getAbsolutePath());*/
        String fileName = args.length > 0 && args[0] != null ? args[0] : "testDataForW1D1.txt";

        try (Stream<String> lines = Files.lines(Paths.get(fileName))) {
            words = lines
                    .flatMap(l -> Arrays.stream(l.split("(\\s|-)")))
                    .map(w -> w.replaceAll("(\\.|\")", ""))
                    .filter(w -> w.matches("[a-zA-Z]+"))
                    .map(w -> new Pair(w))
                    .sorted(new PairSort())
                    .collect(Collectors.toList());
            App.printPairs(words);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void printPairs(List<Pair> pairs) {
        for (Pair word : pairs)
            System.out.println(word);
    }
}
