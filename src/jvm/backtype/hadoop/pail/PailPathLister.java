package backtype.hadoop.pail;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface PailPathLister extends Serializable {
    public List<Path> getPaths(Pail p) throws IOException;
}
