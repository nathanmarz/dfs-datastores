package com.backtype.hadoop.datastores;

import com.backtype.hadoop.datastores.TimeSliceStore.Slice;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.backtype.support.FSTestCase;
import com.backtype.support.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import static backtype.support.TestUtils.*;


public class TimeSliceStoreTest extends FSTestCase {

    public static List readSlice(TimeSliceStore s, Slice slice) throws IOException {
        List ret = new ArrayList();
        Iterator it = s.openRead(slice);
        Object o;
        while(it.hasNext()) {
            ret.add(it.next());
        }
        return ret;
    }

    public static void assertSliceContains(TimeSliceStore s, Slice slice, Object... objs) throws IOException {
        Set objSet = new HashSet(readSlice(s, slice));
        assertEquals(objSet.size(), objs.length);
        for(Object o: objs) {
            assertTrue(objSet.contains(o));
        }
    }

    public static void writeSlice(TimeSliceStore s, Slice slice, Object... objs) throws IOException {
        TypedRecordOutputStream os = s.openWrite(slice);
        for(Object o: objs) {
            os.writeObject(o);
        }
        os.close();
        s.finishSlice(slice);
    }

    public void testReadWrite() throws Exception {
        String tmp1 = getTmpPath(fs, "slices");
        TimeSliceStore<String> sliceStore = TimeSliceStore.create(fs, tmp1, new TimeSliceStringStructure());
        assertNull(sliceStore.maxSliceStartSecs());
        assertNull(sliceStore.minSliceStartSecs());

        try {
            sliceStore.openWrite(Utils.weekStartTime(100), Utils.weekStartTime(100)-1);
            fail("should fail!");
        } catch(IllegalArgumentException e) {

        }

        try {
            sliceStore.openWrite(Utils.weekStartTime(100)-1, Utils.weekStartTime(100));
            fail("should fail!");
        } catch(IllegalArgumentException e) {

        }
        Slice slice = new Slice(Utils.weekStartTime(100), Utils.weekStartTime(100));
        TypedRecordOutputStream os = sliceStore.openWrite(slice);
        os.writeObject("a1");
        os.writeObject("a2");
        os.close();

        try {
            sliceStore.openRead(slice);
            fail("should fail!");
        } catch(IllegalArgumentException e) {

        }
        assertFalse(sliceStore.isSliceExists(slice));
        sliceStore.finishSlice(slice);
        assertTrue(sliceStore.isSliceExists(slice));

        assertSliceContains(sliceStore, slice, "a1", "a2");

        try {
            sliceStore.openWrite(Utils.weekStartTime(99), Utils.weekStartTime(99)+1);
            fail("should fail!");
        } catch(IllegalArgumentException e) {

        }

        Slice slice2 = new Slice(Utils.weekStartTime(100), Utils.weekStartTime(100)+10);
        os = sliceStore.openWrite(slice2);
        os.writeObject("b");
        os.close();
        sliceStore.finishSlice(slice2);

        assertSliceContains(sliceStore, slice, "a1", "a2");

        assertSliceContains(sliceStore, slice2, "b");

        try {
            sliceStore.openWrite(Utils.weekStartTime(100), Utils.weekStartTime(100)+1);
            fail("should fail!");
        } catch(IllegalArgumentException e) {

        }

        assertEquals(Utils.weekStartTime(100)+10, (int) sliceStore.maxSliceStartSecs());
        assertEquals(Utils.weekStartTime(100), (int) sliceStore.minSliceStartSecs());

        try {
            sliceStore.finishSlice(Utils.weekStartTime(300), Utils.weekStartTime(299));
            fail("should fail");
        } catch(IllegalArgumentException e) {

        }

        sliceStore.finishSlice(Utils.weekStartTime(200), Utils.weekStartTime(200) + 21);
        assertEquals(Utils.weekStartTime(200) + 21, (int) sliceStore.maxSliceStartSecs());


    }

    protected static interface AppendOperation {
        public void append(TimeSliceStore dest, TimeSliceStore source) throws IOException;
    }

    public void appendTester(AppendOperation op) throws Exception {
        String path1 = getTmpPath(fs, "sliceStore1");
        String path2 = getTmpPath(fs, "sliceStoreNoAppend");
        String path3 = getTmpPath(fs, "sliceStoreAppend");

        TimeSliceStore base = TimeSliceStore.create(path1, new TimeSliceStringStructure());
        TimeSliceStore invalid = TimeSliceStore.create(path2, new TimeSliceStringStructure());
        TimeSliceStore valid = TimeSliceStore.create(path3, new TimeSliceStringStructure());

        writeSlice(base, new Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa");
        writeSlice(invalid, new Slice(Utils.weekStartTime(89), Utils.weekStartTime(89)+1), "bb");
        writeSlice(valid, new Slice(Utils.weekStartTime(91), Utils.weekStartTime(91)+1), "cc");

        try {
            op.append(base, invalid);
            fail("should fail!");
        } catch(IllegalArgumentException e) {

        }
        assertEquals(1, base.getWeekStarts().size());
        assertEquals(Utils.weekStartTime(90)+1, (int) base.minSliceStartSecs());
        assertEquals(Utils.weekStartTime(90)+1, (int) base.maxSliceStartSecs());

        assertSliceContains(base, new Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa");

        op.append(base, valid);

        assertEquals(2, base.getWeekStarts().size());
        assertEquals(Utils.weekStartTime(90)+1, (int) base.minSliceStartSecs());
        assertEquals(Utils.weekStartTime(91)+1, (int) base.maxSliceStartSecs());
        assertSliceContains(base, new Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa");
        assertSliceContains(base, new Slice(Utils.weekStartTime(91), Utils.weekStartTime(91)+1), "cc");

    }

    public void testCopyAppend() throws Exception {
        appendTester(new AppendOperation() {
            public void append(TimeSliceStore dest, TimeSliceStore source) throws IOException {
                dest.copyAppend(source);
            }
        });
    }

    public void testMoveAppend() throws Exception {
        appendTester(new AppendOperation() {
            public void append(TimeSliceStore dest, TimeSliceStore source) throws IOException {
                dest.moveAppend(source);
            }
        });
    }

    public void testAbsorb() throws Exception {
        appendTester(new AppendOperation() {
            public void append(TimeSliceStore dest, TimeSliceStore source) throws IOException {
                dest.absorb(source);
            }
        });
    }

    public void testConsolidate() throws Exception {
        String tmp = getTmpPath(fs, "sliceStore");

        TimeSliceStore store = TimeSliceStore.create(tmp, new TimeSliceStringStructure());
        writeSlice(store, new Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa", "bb");
        writeSlice(store, new Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+2), "cc");

        Slice slice = new Slice(Utils.weekStartTime(100), Utils.weekStartTime(100));
        TypedRecordOutputStream os = store.openWrite(slice);
        os.writeObject("dd");
        os.close();
        os = store.openWrite(slice);
        os.writeObject("ee");
        os.close();
        store.finishSlice(slice);

        store.consolidate();

        assertSliceContains(store, new Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa", "bb");
        assertSliceContains(store, new Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+2), "cc");
        assertSliceContains(store, new Slice(Utils.weekStartTime(100), Utils.weekStartTime(100)), "dd", "ee");
    }

    public static class TimeSliceStringStructure extends TimeSliceStructure<String> {
        public byte[] serialize(String str) {
            return str.getBytes();
        }

        public String deserialize(byte[] serialized) {
            return new String(serialized);
        }

        public Class getType() {
            return String.class;
        }
    }
}
