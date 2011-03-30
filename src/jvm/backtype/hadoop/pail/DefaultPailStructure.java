package backtype.hadoop.pail;

import java.util.Collections;
import java.util.List;


public class DefaultPailStructure extends BinaryPailStructure {

    public boolean isValidTarget(String... dirs) {
        return true;
    }

    public List<String> getTarget(byte[] object) {
        return Collections.EMPTY_LIST;
    }
}
