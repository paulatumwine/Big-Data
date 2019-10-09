import java.util.ArrayList;
import java.util.List;

public class GroupByPair <T, U> {

    private T key;
    private U values;

    public GroupByPair(T key, U values) {
        this.key = key;
        this.values = values;
    }

    public T getKey() {
        return key;
    }

    public void setKey(T key) {
        this.key = key;
    }

    public U getValues() {
        return values;
    }

    public void setValues(U values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "< " + key + " , " + values + " >";
    }
}
