package backtype.hadoop.datastores;

import backtype.hadoop.pail.CopyArgs;
import backtype.hadoop.pail.Pail;
import backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import backtype.support.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.fs.FileSystem;

public class TimeSliceStore<T> {
    public static class Slice {
        public int weekStart;
        public int sliceStart;

        public Slice(int weekStart, int sliceStart) {
            this.weekStart = weekStart;
            this.sliceStart = sliceStart;
        }

        public Slice(long weekStart, long sliceStart) {
            this.weekStart = (int)weekStart;
            this.sliceStart = (int)sliceStart;
        }
    }

    public static TimeSliceStore create(String path, TimeSliceStoreSpec spec) throws IOException {
        return create(Utils.getFS(path), path, spec);
    }

    public static TimeSliceStore create(FileSystem fs, String path, TimeSliceStoreSpec spec) throws IOException {
        return create(fs, path, spec, true);
    }

    public static TimeSliceStore create(String path) throws IOException {
        return create(Utils.getFS(path), path);
    }

    public static TimeSliceStore create(FileSystem fs, String path) throws IOException {
        return create(fs, path, (TimeSliceStoreSpec) null);
    }

    public static TimeSliceStore create(String path, TimeSliceStructure structure) throws IOException {
        return create(Utils.getFS(path), path, structure);
    }

    public static TimeSliceStore create(FileSystem fs, String path, TimeSliceStructure structure) throws IOException {
        return create(fs, path, new TimeSliceStoreSpec(structure));
    }

    public static TimeSliceStore create(String path, TimeSliceStructure structure, boolean failOnExists) throws IOException {
        return create(Utils.getFS(path), path, structure, failOnExists);
    }

    public static TimeSliceStore create(FileSystem fs, String path, TimeSliceStructure structure, boolean failOnExists) throws IOException {
        return create(fs, path, new TimeSliceStoreSpec(structure), failOnExists);
    }

    public static TimeSliceStore create(String path, boolean failOnExists) throws IOException {
        return create(Utils.getFS(path), path, failOnExists);
    }

    public static TimeSliceStore create(FileSystem fs, String path, boolean failOnExists) throws IOException {
        return create(fs, path, (TimeSliceStoreSpec) null, failOnExists);
    }

    public static TimeSliceStore create(String path, TimeSliceStoreSpec spec, boolean failOnExists) throws IOException {
        return create(Utils.getFS(path), path, spec, failOnExists);
    }

    public static TimeSliceStore create(FileSystem fs, String path, TimeSliceStoreSpec spec, boolean failOnExists) throws IOException {
        if(spec==null) spec = new TimeSliceStoreSpec();
        //TODO: if path is a pail but not the root, it's always an error
        Pail p = Pail.create(fs, path, spec.toPailSpec(), failOnExists);
        return new TimeSliceStore(fs, path);
    }

    private Pail<T> _pail;

    public TimeSliceStore(String path) throws IOException {
        _pail = new Pail<T>(path);
        validate();
    }

    public TimeSliceStore(FileSystem fs, String path) throws IOException {
        _pail = new Pail<T>(fs, path);
        validate();
    }

    private void validate() {
        if(!_pail.atRoot()) {
            throw new IllegalArgumentException("Cannot initialize a TimeSliceStore at that path (pail must be at root)");
        }
    }

    public String getRoot() {
        return _pail.getRoot();
    }

    public List<Integer> getWeekStarts() throws IOException {
        List<String> attrs = _pail.getAttrsAtDir("");
        List<Integer> ret = new ArrayList<Integer>();
        for(String a: attrs) {
            ret.add(Integer.parseInt(a));
        }
        Collections.sort(ret);
        return ret;
    }

    public List<Integer> getSliceStarts(int week) throws IOException {
        List<String> slices = _pail.getMetadataFileNames("" + week);
        List<Integer> ret = new ArrayList<Integer>();
        for(String a: slices) {
            ret.add(Integer.parseInt(a));
        }
        Collections.sort(ret);
        return ret;
    }

    public Integer maxSliceStartSecs(int week) throws IOException {
        List<Integer> slices = getSliceStarts(week);
        if(slices.size()==0) return null;
        return slices.get(slices.size()-1);
    }

    public Integer minSliceStartSecs(int week) throws IOException {
        List<Integer> slices = getSliceStarts(week);
        if(slices.size()==0) return null;
        return slices.get(0);
    }

    public Integer maxWeekStartSecs() throws IOException {
        List<Integer> weeks = getWeekStarts();
        if(weeks.size()==0) return null;
        return weeks.get(weeks.size()-1);
    }

    public Integer minWeekStartSecs() throws IOException {
        List<Integer> weeks = getWeekStarts();
        if(weeks.size()==0) return null;
        return weeks.get(weeks.size()-1);
    }

    public Integer maxSliceStartSecs() throws IOException {
        List<Integer> weekStarts = getWeekStarts();
        Collections.reverse(weekStarts);
        for(Integer week: weekStarts) {
            Integer maxSliceStart = maxSliceStartSecs(week);
            if(maxSliceStart!=null) return maxSliceStart;
        }
        return null;
    }

    public Integer minSliceStartSecs() throws IOException {
        List<Integer> weekStarts = getWeekStarts();
        for(Integer week: weekStarts) {
            Integer minSliceStart = minSliceStartSecs(week);
            if(minSliceStart!=null) return minSliceStart;
        }
        return null;
    }

    public TypedRecordOutputStream openWrite(Slice slice) throws IOException {
        return openWrite(slice.weekStart, slice.sliceStart);
    }

    public TypedRecordOutputStream openWrite(int weekStart, int sliceStart) throws IOException {
        Integer maxSlice = maxSliceStartSecs();
        if(maxSlice!=null && sliceStart <= maxSlice) {
            throw new IllegalArgumentException("Cannot write to  " + weekStart + "/" + sliceStart + ". A bigger slice already exists.");
        }
        validateSlice(weekStart, sliceStart);
        return _pail.openWrite("" + weekStart + "/" + sliceStart + "/" + UUID.randomUUID().toString(), false);
    }

    public TypedRecordOutputStream openWrite(long weekStart, long sliceStart) throws IOException {
        return openWrite((int) weekStart, (int) sliceStart);
    }

    public Iterator<T> openRead(Slice slice) throws IOException {
        return openRead(slice.weekStart, slice.sliceStart);
    }

    public Iterator<T> openRead(int weekStart, int sliceStart) throws IOException {
        if(!isSliceExists(weekStart, sliceStart)) {
            throw new IllegalArgumentException("Cannot read from non-finished slice");
        }
        return _pail.getSubPail(weekStart, sliceStart).iterator();
    }

    public boolean isSliceExists(Slice slice) throws IOException {
        return isSliceExists(slice.weekStart, slice.sliceStart);
    }


    public boolean isSliceExists(int weekStart, int sliceStart) throws IOException {
        return _pail.getMetadata("" + weekStart + "/" + sliceStart) != null;
    }

    private void validateSlice(int weekStart, int sliceStart) {
        if(!_pail.getSpec().getStructure().isValidTarget("" + weekStart, "" + sliceStart)) {
            throw new IllegalArgumentException("" + weekStart + "/" + sliceStart + " is not a valid slice");
        }
    }

    public List<Slice> getUnfinishedSlices() throws IOException {
       List<Slice> ret = new ArrayList<Slice>();
       for(Integer weekStart: getWeekStarts()) {
           Set<Integer> existingSlices = new HashSet<Integer>(getSliceStarts(weekStart));
           for(String attr: _pail.getAttrsAtDir("" + weekStart)) {
               try {
                    int maybeSlice = Integer.parseInt(attr);
                    if(!existingSlices.contains(maybeSlice)) {
                        ret.add(new Slice(weekStart, maybeSlice));
                    }
               } catch(NumberFormatException nfe) {
                   
               }
           }
       }
       return ret;
    }

    public void finishSlice(Slice slice) throws IOException {
        finishSlice(slice.weekStart, slice.sliceStart);
    }

    public void finishSlice(int weekStart, int sliceStart) throws IOException {
        validateSlice(weekStart, sliceStart);
        _pail.mkAttr("" + weekStart + "/" + sliceStart);
        _pail.writeMetadata("" + weekStart + "/" + sliceStart, "slice");
    }

    public void finishSlice(long weekStart, long sliceStart) throws IOException {
        finishSlice((int) weekStart, (int) sliceStart);
    }

    public void copyAppend(TimeSliceStore other) throws IOException {
        doAppend(other, new AppendFunction() {
            public void append(Pail dest, Pail source, CopyArgs args) throws IOException {
                dest.copyAppend(source, args);
            }            
        });
    }

    public void moveAppend(TimeSliceStore other) throws IOException {
        doAppend(other, new AppendFunction() {
            public void append(Pail dest, Pail source, CopyArgs args) throws IOException {
                dest.moveAppend(source, args);
            }
        });
    }

    public void absorb(TimeSliceStore other) throws IOException {
        doAppend(other, new AppendFunction() {
            public void append(Pail dest, Pail source, CopyArgs args) throws IOException {
                dest.absorb(source, args);
            }
        });        
    }

    /**
     * Should only call this if you know there are no readers.
     */
    public void consolidate() throws IOException {
        _pail.consolidate();
    }

    public void consolidate(Slice slice) throws IOException {
        consolidate(slice.weekStart, slice.sliceStart);
    }

    public void consolidate(int weekStart, int sliceStart) throws IOException {
        _pail.getSubPail(weekStart, sliceStart).consolidate();
    }

    private void doAppend(TimeSliceStore<T> other, AppendFunction function) throws IOException {
        checkAppendValidity(other);
        CopyArgs args = new CopyArgs();
        args.copyMetadata = false;
        function.append(_pail, other._pail, args);
        for(Integer weekStart: other.getWeekStarts()) {
            for(Integer sliceStart: other.getSliceStarts(weekStart)) {
               finishSlice(weekStart, sliceStart);
            }
        }
    }

    private void checkAppendValidity(TimeSliceStore other) throws IOException {
        Integer mymax = maxSliceStartSecs();
        Integer othermin = other.minSliceStartSecs();
        if(mymax==null || othermin==null) return;
        if(othermin <= mymax) {
            throw new IllegalArgumentException("Slice store at " + other.getRoot() + " cannot be appended to " +
                    getRoot() + ". Min slice is greater than max slice.");
        }
    }

    protected static interface AppendFunction {
        public void append(Pail dest, Pail source, CopyArgs args) throws IOException;
    }
}
