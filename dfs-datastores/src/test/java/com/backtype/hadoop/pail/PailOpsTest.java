package com.backtype.hadoop.pail;

import com.backtype.hadoop.RenameMode;
import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.support.FSTestCase;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import static com.backtype.support.TestUtils.*;

public class PailOpsTest extends FSTestCase {
    private void writeStrings(Pail pail, String userfile, String... strs) throws IOException {
        writeStrings(pail, userfile, Arrays.asList(strs));
    }

    private void writeStrings(Pail pail, String userfile, Collection<String> strs) throws IOException {
        RecordOutputStream os = pail.openWrite(userfile);
        for(String s: strs) {
            os.writeRaw(s.getBytes());
        }
        os.close();
    }

    public List<String> readWithIt(Pail<byte[]> pail) {
        List<String> returned = new ArrayList<String>();
        for(byte[] b: pail) {
            returned.add(new String(b));
        }
        return returned;
    }

    public void testDeleteSnapshot() throws IOException {
        String path = getTmpPath(local, "pail");
        String snap = getTmpPath(fs, "snapshot");
        Pail pail = Pail.create(local, path, new StringStructure());
        writeStrings(pail, "aaa", "a", "b", "c", "d", "e");
        writeStrings(pail, "bbb", "1", "2", "3");
        Pail snapPail = pail.snapshot(snap);
        writeStrings(pail, "aaa2", "a1");
        pail.deleteSnapshot(snapPail);
        assertPailContents(pail, "a1");
        List<String> names = pail.getUserFileNames();
        assertEquals("aaa2", names.get(0));
        assertEquals(1, names.size());
    }

    public void testClear() throws IOException {
        String path = getTmpPath(fs, "pail");
        Pail<String> pail = Pail.create(fs, path, PailFormatFactory.getDefaultCopy().setStructure(new TestStructure()));
        Pail<String>.TypedRecordOutputStream os = pail.openWrite();
        os.writeObject("a1");
        os.writeObject("b1");
        os.writeObject("c1");
        os.writeObject("a2");
        os.writeObject("za1");
        os.writeObject("za2");
        os.writeObject("zb1");
        os.writeObject("a7");
        os.writeObject("a8");
        os.writeObject("za3");
        os.close();
        pail.getSubPail("a").clear();
        assertPailContents(pail, "b1", "c1", "za1", "za2", "zb1", "za3");
        pail.clear();
        assertPailContents(pail);
    }

    public void testConsolidationOne() throws Exception {
        String path = getTmpPath(local, "pail");
        Pail pail = Pail.create(local, path);
        writeStrings(pail, "aaa", "a", "b", "c", "d", "e");
        writeStrings(pail, "b/c/ddd", "1", "2", "3");
        writeStrings(pail, "b/c/eee", "aaa", "bbb", "ccc", "ddd", "eee", "fff");
        writeStrings(pail, "f", "z");
        writeStrings(pail, "g", "zz");
        writeStrings(pail, "h", "zzz");
        pail.writeMetadata("a/b/qqq", "lalala");
        pail.writeMetadata("f", "abc");
        pail.consolidate(null);
        assertEquals(1, pail.getUserFileNames().size());
        Set<String> results = new HashSet<String>(readWithIt(pail));
        Set<String> expected = new HashSet<String>(Arrays.asList("a", "b", "c", "d", "e",
                "1", "2", "3","aaa", "bbb", "ccc", "ddd", "eee", "fff","z", "zz", "zzz"));
        assertEquals(expected, results);
        assertEquals("abc", pail.getMetadata("f"));
        assertEquals("lalala", pail.getMetadata("a/b/qqq"));
    }

    public void testConsolidationMany() throws Exception {
        String path = getTmpPath(local, "pail");
        Pail pail = Pail.create(local, path);
        writeStrings(pail, "aaa", "a", "b", "c", "d", "e");
        writeStrings(pail, "b/c/ddd", "1", "2", "3");
        writeStrings(pail, "b/c/eee", "aaa", "bbb", "ccc", "ddd", "eee", "fff");
        writeStrings(pail, "f", "z");
        writeStrings(pail, "g", "zz");
        writeStrings(pail, "h", "zzz");
        long target = local.getContentSummary(pail.toStoredPath("f")).getLength() +
                local.getContentSummary(pail.toStoredPath("g")).getLength() + 1;
        pail.consolidate(null,target);
        assertTrue(pail.getUserFileNames().size() < 6 && pail.getUserFileNames().size() > 1);
        Set<String> results = new HashSet<String>(readWithIt(pail));
        Set<String> expected = new HashSet<String>(Arrays.asList("a", "b", "c", "d", "e",
                "1", "2", "3","aaa", "bbb", "ccc", "ddd", "eee", "fff","z", "zz", "zzz"));
        assertEquals(expected, results);
    }

    public void testConsolidateStructured() throws Exception {
        String path = getTmpPath(fs, "pail");
        Pail<String> pail = Pail.create(fs, path, PailFormatFactory.getDefaultCopy().setStructure(new TestStructure()));
        Pail<String>.TypedRecordOutputStream os = pail.openWrite();
        os.writeObject("a1");
        os.writeObject("b1");
        os.writeObject("c1");
        os.writeObject("a2");
        os.writeObject("za1");
        os.writeObject("za2");
        os.writeObject("zb1");
        os.close();
        os = pail.openWrite();
        os.writeObject("a7");
        os.writeObject("a8");
        os.writeObject("za3");
        os.close();
        pail.consolidate(null);
        assertPailContents(pail, "a1", "b1", "c1", "a2", "za1", "za2", "zb1", "a7", "a8", "za3");
        assertPailContents(pail.getSubPail("a"), "a1", "a2", "a7", "a8");
        assertPailContents(pail.getSubPail("z"), "za1", "za2", "zb1", "za3");
        assertPailContents(pail.getSubPail("z/a"), "za1", "za2", "za3");
    }

    protected static interface AppendOperation {
        public void append(Pail into, Pail data, int renameMode) throws IOException;
        public void append(Pail into, Pail data, CopyArgs args) throws IOException;
        public boolean canAppendDifferentFormats();
    }

    private void basicAppendTest(AppendOperation op) throws Exception {
        String path1 = getTmpPath(fs, "pail");
        String path2 = getTmpPath(fs, "pail2");

        //test non structured append
        Pail p1 = Pail.create(fs, path1, new StringStructure());
        Pail p2 = Pail.create(fs, path2, new StringStructure());
        emitObjectsToPail(p1, "aaa", "bbb", "ccc", "ddd");
        emitObjectsToPail(p1, "eee", "fff", "ggg");
        emitObjectsToPail(p2, "hhh");
        emitObjectsToPail(p2, "iii");
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        assertPailContents(p1, "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii");
    }

    private void renameAndStructureAppendTest(AppendOperation op, PailSpec spec1, PailSpec spec2) throws Exception {
        String path1 = getTmpPath(fs, "pail");
        String path2 = getTmpPath(fs, "pail2");

        spec1.setStructure(new StringStructure());
        spec2.setStructure(new StringStructure());
        //test non structured append
        Pail p1 = Pail.create(fs, path1, spec1);
        Pail p2 = Pail.create(fs, path2, spec2);
        emitToPail(p1, "file1", "a");
        emitToPail(p1, "1/file1", "b");
        emitToPail(p1, "1/2/file1", "c");
        emitToPail(p1, "2/file1", "d");
        emitToPail(p2, "file1", "e");
        emitToPail(p2, "1/file2", "f");
        emitToPail(p2, "3/file1", "g");
        emitToPail(p2, "1/file1", "h");

        try {
            op.append(p1, p2, RenameMode.NO_RENAME);
            fail("should throw exception");
        } catch(Exception e) {

        }
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        assertPailContents(p1, "a", "b", "c", "d", "e", "f", "g", "h");
        assertPailContents(p1.getSubPail("1"), "b", "c", "f", "h");
        Set<String> filenames = new HashSet<String>(p1.getUserFileNames());
        assertEquals(8, filenames.size());
        assertTrue(filenames.remove("file1"));
        assertTrue(filenames.remove("1/file1"));
        assertTrue(filenames.remove("1/2/file1"));
        assertTrue(filenames.remove("1/file2"));
        assertTrue(filenames.remove("2/file1"));
        assertTrue(filenames.remove("3/file1"));

        Set<String> parents = new HashSet<String>();
        for(String f: filenames) {
            parents.add(new Path(f).getParent().toString());
        }
        assertTrue(parents.contains(""));
        assertTrue(parents.contains("1"));

        RecordInputStream in = p1.openRead("file1");
        String s = new String(in.readRawRecord());
        assertTrue(in.readRawRecord()==null);
        in.close();
        assertEquals("a", s);

    }

    public void appendTypeCompatibilityTest(AppendOperation op) throws Exception {
        String path1 = getTmpPath(fs, "pail");
        String path2 = getTmpPath(fs, "pail2");
        Pail p1 = Pail.create(fs, path1);
        Pail p2 = Pail.create(fs, path2, new StringStructure());
        emitToPail(p1, "file1", "a", "b");
        emitObjectsToPail(p2, "b");
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        try {
            op.append(p2, p1, RenameMode.RENAME_IF_NECESSARY);
            fail("should throw exception");
        } catch(IllegalArgumentException e) {

        }
    }

    public void structureProtectionTest(AppendOperation op) throws Exception {
        String path1 = getTmpPath(fs, "pail");
        String path2 = getTmpPath(fs, "pail2");
        Pail p1 = Pail.create(fs, path1, new TestStructure());
        Pail p2 = Pail.create(fs, path2, new StringStructure());
        emitObjectsToPail(p1, "a1", "a2", "b1", "c1", "z11", "z12", "z21");
        emitObjectsToPail(p2, "c1", "d1", "e1");
        try {
            op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
            fail("should throw exception");
        } catch(IllegalArgumentException e) {

        }

        try {
            op.append(p1.getSubPail("z"), p2, RenameMode.RENAME_IF_NECESSARY);
            fail("should throw exception");
        } catch(IllegalArgumentException e) {

        }
        assertPailContents(p1, "a1", "a2", "b1", "c1", "z11", "z12", "z21");
        op.append(p1.getSubPail("a"), p2, RenameMode.RENAME_IF_NECESSARY);
        assertPailContents(p1, "a1", "a2", "b1", "c1", "z11", "z12", "z21", "c1", "d1", "e1");
        assertPailContents(p1.getSubPail("a"), "a1", "a2", "c1", "d1", "e1");



        path1 = getTmpPath(fs, "pail");
        path2 = getTmpPath(fs, "pail2");
        p1 = Pail.create(fs, path1, new TestStructure());
        p2 = Pail.create(fs, path2, new StringStructure());
        emitObjectsToPail(p1, "b1", "za1");
        emitToPail(p2, "a/file1", "a1", "a2");
        emitToPail(p2, "b/file1", "b2");
        emitToPail(p2, "z/a/file111111", "za2");
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        assertPailContents(p1, "b1", "za1", "a1", "a2", "b2", "za2");
        assertPailContents(p1.getSubPail("b"), "b1", "b2");

        path2 = getTmpPath(fs, "pail2");
        p2 = Pail.create(fs, path2, new StringStructure());
        emitToPail(p2, "z/b/file1", "zb1");
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        assertPailContents(p1.getSubPail("z/b"), "zb1");
    }

    public void metadataConflictTest(AppendOperation op) throws IOException {
        String path1 = getTmpPath(fs, "pail");
        String path2 = getTmpPath(fs, "pail2");

        //test non structured append
        Pail p1 = Pail.create(fs, path1, new StringStructure());
        Pail p2 = Pail.create(fs, path2, new StringStructure());

        emitToPail(p1, "file1", "aaa");
        emitToPail(p2, "file1", "bbb");
        emitToPail(p2, "file2", "ccc");
        p1.writeMetadata("file1", "M1");
        p1.writeMetadata("a/b", "M2");
        p2.writeMetadata("file1", "M3");
        p2.writeMetadata("a/c", "M4");
        CopyArgs args = new CopyArgs();
        args.copyMetadata = false;
        op.append(p1, p2, args);

        assertPailContents(p1, "aaa", "bbb", "ccc");
        assertEquals("M1", p1.getMetadata("file1"));
        assertEquals("M2", p1.getMetadata("a/b"));
        assertNull(p1.getMetadata("a/c"));


        try {
            op.append(p1, p2, new CopyArgs());
            fail("expected exception");
        } catch(IllegalArgumentException e) {

        }
    }

    public void metadataNonConflictTest(AppendOperation op) throws IOException {
        String path1 = getTmpPath(fs, "pail");
        String path2 = getTmpPath(fs, "pail2");
        String path3 = getTmpPath(fs, "pail3");

        //test non structured append
        Pail p1 = Pail.create(fs, path1, new PailSpec(PailFormatFactory.SEQUENCE_FILE).setStructure(new StringStructure()));
        Pail p2 = Pail.create(fs, path2, new PailSpec(PailFormatFactory.SEQUENCE_FILE).setStructure(new StringStructure()));
        Pail p3 = Pail.create(fs, path3, new PailSpec(PailFormatFactory.SEQUENCE_FILE)
                    .setArg(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_DEFAULT)
                    .setArg(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK)
                    .setStructure(new StringStructure()));

        emitToPail(p1, "meta3", "aaa");
        emitToPail(p2, "file1", "bbb");
        emitToPail(p3, "file2", "ccc");

        p1.writeMetadata("meta1", "M1");
        p2.writeMetadata("a/b/meta2", "M2");
        p3.writeMetadata("meta3", "M3");

        op.append(p1, p2, new CopyArgs());

        assertPailContents(p1, "aaa", "bbb");
        assertEquals("M1", p1.getMetadata("meta1"));
        assertEquals("M2", p1.getMetadata("a/b/meta2"));


        if(op.canAppendDifferentFormats()) {
            System.out.println(p1.getStoredFilesAndMetadata());
            op.append(p1, p3, new CopyArgs());

            assertPailContents(p1, "aaa", "bbb", "ccc");
            assertEquals("M1", p1.getMetadata("meta1"));
            assertEquals("M2", p1.getMetadata("a/b/meta2"));
            assertEquals("M3", p1.getMetadata("meta3"));
        }
    }


    public void appendOperationTest(AppendOperation op) throws Exception {
        basicAppendTest(op);
        if(op.canAppendDifferentFormats())
            renameAndStructureAppendTest(op, new PailSpec(PailFormatFactory.SEQUENCE_FILE),
                    new PailSpec(PailFormatFactory.SEQUENCE_FILE)
                    .setArg(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_DEFAULT)
                    .setArg(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK));

        renameAndStructureAppendTest(op, new PailSpec(PailFormatFactory.SEQUENCE_FILE),
                new PailSpec(PailFormatFactory.SEQUENCE_FILE));
        appendTypeCompatibilityTest(op);
        structureProtectionTest(op);
        metadataConflictTest(op);
        metadataNonConflictTest(op);
    }

    public void testCopyAppend() throws Exception {
        appendOperationTest(new AppendOperation() {
            public void append(Pail into, Pail data, int renameMode) throws IOException {
                into.copyAppend(data, renameMode);
            }

            public void append(Pail into, Pail data, CopyArgs args) throws IOException {
                into.copyAppend(data, args);
            }

            public boolean canAppendDifferentFormats() {
                return true;
            }
        });
    }

    public void testMoveAppend() throws Exception {
        appendOperationTest(new AppendOperation() {
            public void append(Pail into, Pail data, int renameMode) throws IOException {
                into.moveAppend(data, renameMode);
            }

            public void append(Pail into, Pail data, CopyArgs args) throws IOException {
                into.moveAppend(data, args);
            }

            public boolean canAppendDifferentFormats() {
                return false;
            }
        });

        //TODO: test that original pail is now empty
    }

    public void testAbsorb() throws Exception {
        appendOperationTest(new AppendOperation() {
            public void append(Pail into, Pail data, int renameMode) throws IOException {
                into.absorb(data, renameMode);
            }

            public void append(Pail into, Pail data, CopyArgs args) throws IOException {
                into.absorb(data, args);
            }

            public boolean canAppendDifferentFormats() {
                return true;
            }
        });
    }


    public static class StringStructure implements PailStructure<String> {

        public boolean isValidTarget(String... dirs) {
            return true;
        }

        public String deserialize(byte[] serialized) {
            return new String(serialized);
        }

        public byte[] serialize(String object) {
            return object.getBytes();
        }

        public List<String> getTarget(String object) {
            return Collections.EMPTY_LIST;
        }

        @JsonIgnore
        public Class getType() {
            return String.class;
        }

    }
}
