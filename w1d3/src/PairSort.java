import java.util.Comparator;

public class PairSort implements Comparator<Pair> {

    @Override
    public int compare(Pair pair, Pair t1) {
        return pair.getKey().toString().compareTo(t1.getKey().toString());
    }
}
