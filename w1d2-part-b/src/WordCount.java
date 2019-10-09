import java.util.ArrayList;
import java.util.List;

public class WordCount {
    List<Mapper> m = new ArrayList<>();
    List<Reducer> r = new ArrayList<>();

    public WordCount(int m, int r) {
        for (int i = 0; i < m; i++) {
            this.m.add(new Mapper());
        }
        for (int i = 0; i < r; i++) {
            this.r.add(new Reducer());
        }
    }

    int getPartition(String key, int numReducers) {
        return Math.abs(key.hashCode()) % numReducers;
    }

    public List<Mapper> getM() {
        return m;
    }

    public void setM(List<Mapper> m) {
        this.m = m;
    }

    public List<Reducer> getR() {
        return r;
    }

    public void setR(List<Reducer> r) {
        this.r = r;
    }
}
