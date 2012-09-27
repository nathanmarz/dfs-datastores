package backtype.support;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Utils {
    public static final int DAY_SECS = 60*60*24;
    public static final int WEEK_TIME_SECS = DAY_SECS*7;

    public static int toWeek(int secs) {
        return secs / WEEK_TIME_SECS;
    }

    public static long weekStartTime(int week) {
        return ((long) week) * WEEK_TIME_SECS;
    }

    public static int toDay(int secs) {
        return secs / DAY_SECS;
    }

    public static long dayStartTime(int day) {
        return ((long) day) * DAY_SECS;
    }

    /**
     * Return true or false if the input is a long
     * @param input
     * @return boolean
     */
    public static boolean isLong(String input) {
        try {
            Long.parseLong(input);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static byte[] getBytes(BytesWritable bw) {
        byte[] padded = bw.getBytes();
        byte[] ret = new byte[bw.getLength()];
        System.arraycopy(padded, 0, ret, 0, ret.length);
        return ret;
    }

    public static int currentTimeSecs() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public static <T> String join(T[] arr, String sep) {
        String ret = "";
        for(int i=0; i < arr.length; i++) {
            ret = ret + arr[i];
            if(i < arr.length-1) {
                ret = ret + sep;
            }            
        }
        return ret;
    }

    public static void setObject(JobConf conf, String key, Object o) {
        conf.set(key, StringUtils.byteToHexString(serialize(o)));
    }

    public static Object getObject(JobConf conf, String key) {
        String s = conf.get(key);
        if(s==null) return null;
        byte[] val = StringUtils.hexStringToByte(s);
        return deserialize(val);
    }

    public static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static Object deserialize(byte[] serialized) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object ret = ois.readObject();
            ois.close();
            return ret;
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        } catch(ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static String makeRelative(Path root, Path full) {
        List<String> rootc = componentize(root.toString());
        List<String> fullc = componentize(full.toString());
        List<String> rel = stripRoot(rootc, fullc);
        return join(rel, "/");
    }

    public static List<String> componentize(String ps) {
        Path p = new Path(ps);
        List<String> dirs = new ArrayList<String>();
        do {
            dirs.add(0, p.getName());
        } while((p = p.getParent()) != null);
        dirs.remove(0); // first one will be empty string for root
        return dirs;
    }

    public static List<String> stripRoot(List<String> rootComponents, List<String> fullComponents) {
       List<String> full = new ArrayList<String>(fullComponents);
       List<String> root = new ArrayList<String>(rootComponents);
       while(root.size()>0) {
           String f = full.remove(0);
           String r = root.remove(0);
           if(!r.equals(f)) throw new IllegalStateException(
                   fullComponents.toString() + " is not within root " + rootComponents.toString());
       }
       return full;
    }

    public static <T> String join(Iterable<T> coll, String sep) {
        Iterator<T> it = coll.iterator();
        String ret = "";
        while(it.hasNext()) {
            ret = ret + it.next();
            if(it.hasNext()) {
                ret = ret + sep;
            }
        }
        return ret;
    }

    public static int fill(InputStream is, byte[] buffer) throws IOException {
        int off = 0;
        while(off < buffer.length) {
            int amt = is.read(buffer, off, buffer.length-off);
            if(amt<=0) break;
            off+=amt;

        }
        return off;
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean firstNBytesSame(FileSystem fs1, Path p1, FileSystem fs2, Path p2, long n) throws IOException {
        FSDataInputStream f1 = fs1.open(p1);
        FSDataInputStream f2 = fs2.open(p2);
        byte[] buffer1 = new byte[1024*1024];
        byte[] buffer2 = new byte[1024*1024];
        long total = 0;
        try {
            while(total < n) {
                long left = n - total;
                int amt = fill(f1, buffer1);
                int amt2 = fill(f2, buffer2);
                if(amt<=left && amt2<=left && amt!=amt2) {
                    return false;
                }
                if(amt==0 || amt2==0) {
                    return amt == amt2;
                }
                for(int i=0; i<Math.min(amt, left); i++) {
                    if(buffer1[i]!=buffer2[i]) return false;
                }
                total+=amt;
            }
            return true;
        } finally {
            f1.close();
            f2.close();
        }
    }

    public static boolean hasScheme(String path) {
        return getScheme(path) != null;
    }

    public static String getScheme(String path) {
        return new Path(path).toUri().getScheme();
    }

    public static FileSystem getFS(String path) throws IOException {
        return getFS(path, new Configuration());
    }

    public static FileSystem getFS(String path, Configuration conf) throws IOException {
        return new Path(path).getFileSystem(conf);
    }

    public static String stripExtension(String str, String extension) {
        if(!str.endsWith(extension)) {
            throw new IllegalArgumentException("Cannot strip extension " + str + " : " + extension);
        }
        return str.substring(0, str.length()-extension.length());
    }
    
    public static byte[] readFully(InputStream is) throws IOException {
        List<Byte> ret = new ArrayList<Byte>();
        byte[] buffer = new byte[1024];
        while(true) {
            int numRead = is.read(buffer);
            if(numRead==-1) {
                break;
            } else {
                for(int i=0; i<numRead; i++) {
                    ret.add(buffer[i]);
                }
            }
        }
        byte[] arr = new byte[ret.size()];
        for(int i=0; i<ret.size(); i++) {
            arr[i] = ret.get(i);
        }
        return arr;
    }
}