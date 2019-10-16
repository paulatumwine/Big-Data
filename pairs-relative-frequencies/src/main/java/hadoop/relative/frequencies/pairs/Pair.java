package hadoop.relative.frequencies.pairs;

public class Pair<T, U> {

    private T key;
    private U value;

    Pair(T key, U value) {
        this.key = key;
        this.value = value;
    }

    T getKey() {
        return key;
    }

    public void setKey(T key) {
        this.key = key;
    }

    U getValue() {
        return value;
    }

    public void setValue(U value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "< " + key + " , " + value + " >";
    }
}
