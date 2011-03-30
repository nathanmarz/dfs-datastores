package backtype.hadoop.pail;


public class CopyArgs {
    public Integer renameMode = null;
    public boolean copyMetadata = true;
    public boolean force = false;

    public CopyArgs(CopyArgs other) {
        this.renameMode = other.renameMode;
        this.copyMetadata = other.copyMetadata;
        this.force = other.force;
    }

    public CopyArgs() {
        
    }
}
