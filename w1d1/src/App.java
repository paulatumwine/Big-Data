import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class App {

    static List<Pair> words = new ArrayList<>();

    public static void main(String[] args) {
        /*System.out.println(new File("").getAbsolutePath());*/
        String fileName = args.length > 0 && args[0] != null ? args[0] : "testDataForW1D1.txt";
        try (Scanner scanner = new Scanner(new File(fileName)).useDelimiter("\\s")) {
            while (scanner.hasNext()) {
                String current = scanner.next();
                if (current.matches("[a-zA-Z\\-]+")) {
                    if (current.contains("-")) {
                        for (String part : current.split("-"))
                            words.add(new Pair(part));
                        continue;
                    }
                    words.add(new Pair(current));
                }
            }
            Collections.sort(words, new PairSort());
            App.printPairs(words);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    static void printPairs(List<Pair> pairs) {
        for (Pair word : pairs)
            System.out.println(word);
    }
}
