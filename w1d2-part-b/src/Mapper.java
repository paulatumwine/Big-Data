import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Mapper {

    Function<List<String>, List<Pair>> mapper = lines -> lines.stream()
            .flatMap(l -> Arrays.stream(l.split("(\\s|-)")))
            .map(w -> w.replaceAll("(,|\")", ""))
            .filter(w -> w.matches("^[a-zA-Z]+\\.?$"))
            .map(w -> w.replaceAll("\\.", ""))
            .map(w -> new Pair(w.toLowerCase()))
            .sorted(new PairSort())
            .collect(Collectors.toList());

    List<Pair> map(List<String> words) {
        return mapper.apply(words);
    }
}

