package com.backtype.hadoop.pail;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PailRecordInfo implements Writable {
    private String fullPath;
    private String pailRelativePath;
    private long splitStartOffset;
    private int recordsToSkip;

    public PailRecordInfo() {
    }

    public PailRecordInfo(String fullPath, String pailRelativePath, long splitStartOffset, int recordsToSkip) {
        this.fullPath = fullPath;
        this.pailRelativePath = pailRelativePath;
        this.splitStartOffset = splitStartOffset;
        this.recordsToSkip = recordsToSkip;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, fullPath);
        WritableUtils.writeString(out, pailRelativePath);
        WritableUtils.writeVLong(out, splitStartOffset);
        WritableUtils.writeVInt(out, recordsToSkip);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fullPath = WritableUtils.readString(in);
        pailRelativePath = WritableUtils.readString(in);
        splitStartOffset = WritableUtils.readVLong(in);
        recordsToSkip = WritableUtils.readVInt(in);
    }

    public String getFullPath() {
        return fullPath;
    }

    public String getPailRelativePath() {
        return pailRelativePath;
    }

    public long getSplitStartOffset() {
        return splitStartOffset;
    }

    public int getRecordsToSkip() {
        return recordsToSkip;
    }

    public void setFullPath(String fullPath) {
        this.fullPath = fullPath;
    }

    public void setPailRelativePath(String pailRelativePath) {
        this.pailRelativePath = pailRelativePath;
    }

    public void setSplitStartOffset(long splitStartOffset) {
        this.splitStartOffset = splitStartOffset;
    }

    public void setRecordsToSkip(int recordsToSkip) {
        this.recordsToSkip = recordsToSkip;
    }
}
