import java.util.Comparator;

public class PairSort implements Comparator<Pair> {

    @Override
    public int compare(Pair pair, Pair t1) {
        return pair.getKey().toUpperCase().compareTo(t1.getKey().toUpperCase());
    }
}
