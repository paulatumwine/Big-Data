import java.util.ArrayList;
import java.util.List;

public class GroupByPair {

    private String key;
    private List<Integer> values = new ArrayList<>();

    public GroupByPair(String key) {
        this.key = key;
        this.values.add(1);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<Integer> getValues() {
        return values;
    }

    public void setValues(List<Integer> values) {
        this.values = values;
    }

    public void addValue() {
        this.values.add(1);
    }

    @Override
    public String toString() {
        return "< " + key + " , " + values + " >";
    }
}
