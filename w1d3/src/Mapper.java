import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Mapper {

    /*// initializes the mapper
    Map<Character, List<Integer>> assoc = new HashMap<>();

    Function<List<String>, List<String>> mapper = lines -> lines.stream()
            .flatMap(l -> Arrays.stream(l.split("(\\s|-)")))
            .map(w -> w.replaceAll("(,|\")", ""))
            .filter(w -> w.matches("^[a-zA-Z]+\\.?$"))
            .map(w -> w.replaceAll("\\.", ""))
            .collect(Collectors.toList());

    BiFunction<List<String>, Character, Long> sameStartChar = (words, c) -> words
            .stream()
            .filter(w -> w.toLowerCase().charAt(0) == c)
            .count();

    Mapper map(List<String> lines) {
        List<String> words = mapper.apply(lines);
        for (String word: words) {
            Character c = word.toLowerCase().charAt(0);
            Integer len = word.length();
            *//*Integer count = sameStartChar.apply(words, c).intValue(); *//*
            List<Integer> pair = new ArrayList<>(2);
            if (assoc.containsKey(c)) {
                pair.add(len + assoc.get(c).get(0));
                pair.add(1 + assoc.get(c).get(1));
            } else {
                pair.add(len);
                pair.add(1);
            }
            assoc.put(c, pair);
        }
        return this;
    }

    Map<Character, List<Integer>> emit() {
        return assoc;
    }*/

    // initializes the mapper
    List<Pair<Character, List<Integer>>> assoc = new ArrayList<>();

    Function<List<String>, List<String>> mapper = lines -> lines.stream()
            .flatMap(l -> Arrays.stream(l.split("(\\s|-)")))
            .map(w -> w.replaceAll("(,|\")", ""))
            .filter(w -> w.matches("^[a-zA-Z]+\\.?$"))
            .map(w -> w.replaceAll("\\.", ""))
            .collect(Collectors.toList());

    BiFunction<List<String>, Character, Long> sameStartChar = (words, c) -> words
            .stream()
            .filter(w -> w.toLowerCase().charAt(0) == c)
            .count();

    Mapper map(List<String> lines) {
        List<String> words = mapper.apply(lines);
        for (String word: words) {
            Character c = word.toLowerCase().charAt(0);
            Integer len = word.length();
            /*Integer count = sameStartChar.apply(words, c).intValue(); */
            List<Integer> pair = new ArrayList<>(2);
            Optional<Pair<Character, List<Integer>>> elem = assoc.stream().filter(p -> p.getKey() == c).findFirst();
            if (elem.isPresent()) {
                pair.add(len + elem.get().getValue().get(0));
                pair.add(1 + elem.get().getValue().get(1));
                assoc.set(assoc.indexOf(elem.get()), new Pair<>(c, pair));
            } else {
                pair.add(len);
                pair.add(1);
                assoc.add(new Pair(c, pair));
            }
        }
        return this;
    }

    List<Pair<Character, List<Integer>>> emit() {
        return assoc;
    }

}

