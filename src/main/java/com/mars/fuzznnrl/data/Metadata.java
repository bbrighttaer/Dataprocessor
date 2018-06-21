package com.mars.fuzznnrl.data;

/**
 * Meta of each file submitted to the pipe layer
 */
public class Metadata {
    private final long size;
    private final long numLines;
    private final String filepath;
    private final String processedStorageDir;

    public Metadata(long size, long numLines, String filepath, String processedStorageDir) {
        this.size = size;
        this.numLines = numLines;
        this.filepath = filepath;
        this.processedStorageDir = processedStorageDir;
    }

    public long getSize() {
        return size;
    }

    public long getNumLines() {
        return numLines;
    }

    public String getFilepath() {
        return filepath;
    }

    public String getProcessedStorageDir() {
        return processedStorageDir;
    }
}
