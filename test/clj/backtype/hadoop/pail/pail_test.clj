(ns backtype.hadoop.pail.pail-test)

;; package backtype.hadoop.pail;

;; import backtype.hadoop.formats.RecordInputStream;
;; import backtype.hadoop.formats.RecordOutputStream;
;; import java.io.IOException;
;; import java.util.ArrayList;
;; import java.util.Arrays;
;; import java.util.Collection;
;; import java.util.HashSet;
;; import java.util.List;
;; import java.util.Set;
;; import junit.framework.TestCase;
;; import org.apache.hadoop.conf.Configuration;
;; import org.apache.hadoop.fs.FileSystem;
;; import org.apache.hadoop.fs.Path;
;; import static backtype.support.TestUtils.*;


;; public class PailTest extends TestCase {
;;     FileSystem local;
    
;;     public PailTest() throws IOException {
;;         local = FileSystem.get(new Configuration());
;;     }

;;     public void testCreation() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path);
;;         assertTrue(local.exists(new Path(path)));
;;         assertTrue(local.exists(new Path(path, Pail.META)));
;;         try {
;;             Pail.create(local, path, true);
;;             fail("should fail");
;;         } catch(Exception e) {}
;;         deletePath(local, path);
;;         PailSpec spec = new PailSpec(PailFormatFactory.SEQUENCE_FILE)
;;                 .setArg(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK)
;;                 .setArg(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_GZIP);
;;         Pail.create(local, path, spec);
;;         pail = new Pail(local, path);
;;         assertEquals(spec, pail.getSpec());

;;         Pail.create(local, path + "/a/b", false);
;;         assertTrue(local.exists(new Path(path, "a/b")));

;;         try {
;;             new Pail(local, path + "/c");
;;             fail("should fail");
;;         } catch(IllegalArgumentException e) {
            
;;         }
;;     }

;;     public void testReadingWriting() throws Exception {
;;         readingWritingTestHelper(null);
;;         readingWritingTestHelper(new PailSpec(PailFormatFactory.SEQUENCE_FILE)
;;                 .setArg(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK)
;;                 .setArg(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_DEFAULT));
;;     }

;;     private void emitToPail(Pail pail, String file, byte[]... records) throws Exception {
;;         RecordOutputStream os = pail.openWrite(file);
;;         for(byte[] r: records) {
;;             os.writeRaw(r);
;;         }
;;         os.close();
;;     }

;;     private void checkContains(Pail pail, String file, byte[]... expected) throws Exception {
;;         RecordInputStream is = pail.openRead(file);
;;         List<byte[]> records = new ArrayList<byte[]>();
;;         while(true) {
;;             byte[] arr = is.readRawRecord();
;;             if(arr==null) break;
;;             records.add(arr);
;;         }
;;         assertEquals(expected.length, records.size());
;;         for(int i=0; i < expected.length; i++) {
;;             assertArraysEqual(expected[i], records.get(i));
;;         }

;;     }

;;     private void readingWritingTestHelper(PailSpec spec) throws Exception {
;;         byte[] r1 = new byte[] {1, 2, 3, 10};
;;         byte[] r2 = new byte[] {100};
;;         byte[] r3 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
;;         byte[] r4 = new byte[] {};

;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path, spec);
;;         emitToPail(pail, "a", r1, r2, r3);
;;         emitToPail(pail, "a/b/c", r4);
;;         emitToPail(pail, "ddd", r3, r4, r1);

;;         checkContains(pail, "a", r1, r2, r3);
;;         checkContains(pail, "a/b/c", r4);
;;         checkContains(pail, "ddd", r3, r4, r1);

;;         Set<String> userfiles = new HashSet<String>(pail.getUserFileNames());
;;         assertEquals(3, userfiles.size());
;;         assertTrue(userfiles.contains("a"));
;;         assertTrue(userfiles.contains("a/b/c"));
;;         assertTrue(userfiles.contains("ddd"));
;;     }

;;     private void writeStrings(Pail pail, String userfile, String... strs) throws IOException {
;;         writeStrings(pail, userfile, Arrays.asList(strs));
;;     }

;;     private void writeStrings(Pail pail, String userfile, Collection<String> strs) throws IOException {
;;         RecordOutputStream os = pail.openWrite(userfile);
;;         for(String s: strs) {
;;             os.writeRaw(s.getBytes());
;;         }
;;         os.close();
;;     }

;;     public List<String> readWithIt(Pail<byte[]> pail) {
;;         List<String> returned = new ArrayList<String>();
;;         for(byte[] b: pail) {
;;             returned.add(new String(b));
;;         }
;;         return returned;
;;     }

;;     public void testIterator() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path);
;;         Set<String> records = new HashSet<String>();
;;         records.add("aaaaa");
;;         records.add("bb");
;;         records.add("cccc");
;;         records.add("ddizizijjsjs");
;;         records.add("oqoqoqoq");
;;         writeStrings(pail, "a", records);
;;         writeStrings(pail, "a/b/c/d/eee", "nathan");
;;         records.add("nathan");
;;         Set<String> returned = new HashSet<String>(readWithIt(pail));
;;         assertEquals(records, returned);
;;     }

;;     public void testAtomicity() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path);
;;         RecordOutputStream os = pail.openWrite("aaa");
;;         assertEquals(0, pail.getUserFileNames().size());
;;         os.writeRaw(new byte[] {1, 2, 3});
;;         os.close();
;;         assertEquals(1, pail.getUserFileNames().size());
;;     }

;;     public void checkStoredFiles(Pail pail, String... expected) throws Exception {
;;         Set<Path> paths = new HashSet<Path>(pail.getStoredFiles());
;;         assertEquals(expected.length, paths.size());
;;         for(String e: expected) {
;;             assertTrue(e, paths.contains(new Path(e + Pail.EXTENSION)));
;;         }
;;     }

;;     public void checkUserFiles(Pail pail, String... expected) throws Exception {
;;         Set<String> files = new HashSet<String>(pail.getUserFileNames());
;;         assertEquals(expected.length, files.size());
;;         for(String e: expected) {
;;             assertTrue(e, files.contains(e));
;;         }
;;     }

;;     public void testStoredFiles() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path);
;;         emitToPail(pail, "a/b/c", new byte[] {1});
;;         checkStoredFiles(pail, path + "/a/b/c");
;;         emitToPail(pail, "e", new byte[] {1});
;;         checkStoredFiles(pail, path + "/a/b/c", path + "/e");
;;         emitToPail(pail, "100aaa", new byte[] {1});
;;         emitToPail(pail, "101/202/303", new byte[] {1});
;;         checkStoredFiles(pail, path + "/a/b/c", path + "/e", path + "/100aaa", path + "/101/202/303");
;;     }

;;     public void testSubPail() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path);
;;         emitToPail(pail, "a/b/c", new byte[] {1});
;;         emitToPail(pail, "e", new byte[] {1});
;;         checkUserFiles(pail, "a/b/c", "e");
;;         Pail s1 = pail.getSubPail("a");
;;         checkUserFiles(s1, "b/c");
;;         Pail s2 = new Pail(local, path + "/a/b");
;;         checkUserFiles(s2, "c");

;;         pail.getSubPail("asdasdasdasd");
;;         assertTrue(local.exists(new Path(path, "asdasdasdasd")));
;;     }

;;     public void testIsEmpty() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path);
;;         assertTrue(pail.isEmpty());
;;         emitToPail(pail, "aaa");
;;         assertTrue(pail.isEmpty());
;;         emitToPail(pail, "bbb", new byte[] {1, 2, 3});
;;         assertFalse(pail.isEmpty());
;;         emitToPail(pail, "ccc", new byte[] {1, 2, 3});
;;         assertFalse(pail.isEmpty());
;;         pail.delete("bbb");
;;         assertFalse(pail.isEmpty());
;;         pail.delete("ccc");
;;         assertTrue(pail.isEmpty());
;;         pail.delete("aaa");
;;         assertTrue(pail.isEmpty());
;;     }

;;     public void testStructureConstructor() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail p = Pail.create(local, path, new TestStructure());
;;         PailSpec spec = p.getSpec();
;;         assertNotNull(spec.getName());
;;         assertEquals(TestStructure.class, spec.getStructure().getClass());
;;         //shouldn't throw exceptions...
;;         Pail.create(local, path, new TestStructure(), false);
;;         Pail.create(local, path, false);
;;         try {
;;             Pail.create(local, path, new DefaultPailStructure(), false);
;;             fail("should throw exception");
;;         } catch(IllegalArgumentException e) {

;;         }
;;         path = getTmpPath(local, "pail");
;;         Pail.create(local, path);
;;         try {
;;             Pail.create(local, path, new TestStructure(), false);
;;             fail("should throw exception");
;;         } catch(IllegalArgumentException e) {
            
;;         }

;;     }

;;     protected List<byte[]> getRecords(Pail p, String userfile) throws Exception {
;;         List<byte[]> ret = new ArrayList<byte[]>();
;;         RecordInputStream is = p.openRead(userfile);
;;         byte[] record;
;;         while((record = is.readRawRecord())!=null) {
;;             ret.add(record);
;;         }
;;         is.close();
;;         return ret;
;;     }

;;     public void testStructured() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail<String> pail = Pail.create(local, path, PailFormatFactory.getDefaultCopy().setStructure(new TestStructure()));
;;         Pail<String>.TypedRecordOutputStream os = pail.openWrite();
;;         os.writeObject("a1");
;;         os.writeObject("b1");
;;         os.writeObject("c1");
;;         os.writeObject("a2");
;;         os.writeObject("za1");
;;         os.writeObject("za2");
;;         os.writeObject("zb1");
;;         os.close();

;;         pail = new Pail(local, path);
;;         assertPailContents(pail, "a1", "b1", "c1", "a2", "za1", "za2", "zb1");
;;         assertPailContents(pail.getSubPail("a"), "a1", "a2");
;;         assertPailContents(pail.getSubPail("a/1"));
;;         assertPailContents(pail.getSubPail("z"), "za1", "za2", "zb1");
;;         assertPailContents(pail.getSubPail("z/a"), "za1", "za2");

;;         Pail a = new Pail(local, path + "/a");
;;         os = a.openWrite();
;;         os.writeObject("a2222");
;;         try {
;;             os.writeObject("zzz");
;;             fail("should fail");
;;         } catch(IllegalStateException e) {
            
;;         }
;;         os.close();

;;         assertPailContents(pail, "a1", "b1", "c1", "a2", "za1", "za2", "zb1", "a2222");
;;         assertPailContents(pail.getSubPail("a"), "a1", "a2", "a2222");
;;         assertPailContents(pail.getSubPail("a/1"));
;;         assertPailContents(pail.getSubPail("z"), "za1", "za2", "zb1");
;;         assertPailContents(pail.getSubPail("z/a"), "za1", "za2");

;;     }

;;     public void testMetadata() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path);
;;         emitToPail(pail, "aaa", new byte[] {1});
;;         pail.writeMetadata("aaa", "lalala");
;;         List<byte[]> recs = getRecords(pail, "aaa");
;;         assertEquals(1, recs.size());
;;         assertTrue(Arrays.equals(recs.get(0), new byte[] {1}));
;;         assertEquals("lalala", pail.getMetadata("aaa"));

;;         pail.writeMetadata("a/b", "whee");
;;         assertEquals("whee", pail.getMetadata("a/b"));
;;         assertEquals("whee", new Pail(local, path).getMetadata("a/b"));

;;         assertNull(pail.getMetadata("bbb"));

;;         List<String> md = pail.getMetadataFileNames();
;;         assertEquals(2, md.size());
;;         assertTrue(md.contains("a/b"));
;;         assertTrue(md.contains("aaa"));
;;     }

;;     public void testAtRoot() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path);
;;         assertTrue(pail.atRoot());
;;         assertFalse(Pail.create(local, path + "/a", false).atRoot());
;;     }

;;     public void testMkAttr() throws Exception {
;;         String path = getTmpPath(local, "pail");
;;         Pail pail = Pail.create(local, path);
;;         pail.mkAttr("a/b");
;;         List<String> attrs = pail.getAttrsAtDir("a");
;;         assertEquals(1, attrs.size());
;;         assertEquals("b", attrs.get(0));
;;         Pail pail2 = new Pail(local, path + "/a");
;;         pail2.mkAttr("b/c");
;;         attrs = pail.getAttrsAtDir("a/b");
;;         assertEquals(1, attrs.size());
;;         assertEquals("c", attrs.get(0));


;;     }
;; }
