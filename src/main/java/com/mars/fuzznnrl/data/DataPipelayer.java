package com.mars.fuzznnrl.data;

import ch.qos.logback.classic.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataPipelayer {
    private static Logger log = (Logger) LoggerFactory.getLogger(DataPipelayer.class);
    private final String file_directory;
    private final String[] extension;
    private int numOfShuffles;
    private int writingBatchSize;

    public DataPipelayer(String file_directory, int numOfShuffles, int writingBatchSize, String... extension) {
        this.file_directory = file_directory;
        this.numOfShuffles = numOfShuffles;
        this.writingBatchSize = writingBatchSize;
        this.extension = extension;
    }

    public void startJob() {
        File dir = new File(file_directory);
        Iterator<File> itr = FileUtils.iterateFiles(dir, extension, false);
        List<PipeTask> taskList = new ArrayList<>();
        while (itr.hasNext())
            taskList.add(new PipeTask(itr.next()));
        log.info("Number of jobs submitted to data pipe-layer: {}", taskList.size());
        ExecutorService exec = Executors.newFixedThreadPool(taskList.size());
        for (PipeTask task : taskList)
            exec.submit(task);
        exec.shutdown();
        while (!exec.isTerminated()) ;
        log.info("Data processing done");
    }

    class Pipes {
        private final File file;
        private final String final_file_prefix;
        private final FileSplitter fileSplitter;

        Pipes(File file, String final_file_prefix, FileSplitter fileSplitter) {
            this.file = file;
            this.final_file_prefix = final_file_prefix;
            this.fileSplitter = fileSplitter;
        }

        /**
         * Pipe 1
         * Computes the index of the file
         *
         * @return All created files
         */
        List<LineInfo> pipe1() throws IOException {
            try (RandomAccessFile file = new RandomAccessFile(this.file, "r")) {
                List<LineInfo> lineInfoList = new ArrayList<>();
                int skip = 0;
                String line;
                while ((line = file.readLine()) != null) {
                    lineInfoList.add(new LineInfo(skip, line.length()));
                    long pointerIndex = file.getFilePointer();
                    int gap = (int) (pointerIndex - (skip + line.length()));
                    skip += line.length() + gap;
                }
                return lineInfoList;
            }
        }

        /**
         * Pipe 2
         * Shuffle filenames
         *
         * @return Shuffled list
         */
        List<LineInfo> pipe2(List<LineInfo> lineInfoList) {
            for (int i = 0; i < numOfShuffles; i++) {
                Collections.shuffle(lineInfoList);
            }
            return lineInfoList;
        }

        /**
         * Pipe 3
         * Read each line from the file and aggregate them in a shuffled file
         */
        Metadata pipe3(List<LineInfo> lineInfoList) {
            LocalDate now = LocalDate.now();
            String processed_data = String.format("processed_data_%s", now.toString());
            File shuffledFile = new File(processed_data + "/" + final_file_prefix + "_shuffled."
                    + FilenameUtils.getExtension(file.getName()));
            try (RandomAccessFile accessFile = new RandomAccessFile(this.file, "r")) {
                FileUtils.forceMkdir(new File(processed_data));
                List<String> lines = new ArrayList<>();
                for (LineInfo lineInfo : lineInfoList) {
                    accessFile.seek(lineInfo.skip);
                    String s = accessFile.readLine().trim();
                    lines.add(s);
                    if ((lines.size() % writingBatchSize) == 0) {
                        FileUtils.writeLines(shuffledFile, lines, true);
                        lines.clear();
                    }
                }
                if (lines.size() > 0) {
                    String lastLine = lines.remove(lines.size() - 1);
                    FileUtils.writeLines(shuffledFile, lines, true);
                    lines.clear();
                    lines.add(lastLine);//ensures there is no empty line after the last line
                    FileUtils.writeLines(shuffledFile, lines, "", true);
                }
            } catch (Exception e) {
                log.error("Error processing {} in pipe 3 : {}", this.file.getName(), e.getMessage());
            }
            return new Metadata(FileUtils.sizeOf(shuffledFile), lineInfoList.size(), shuffledFile.getAbsolutePath(),
                    FilenameUtils.getFullPath(shuffledFile.getAbsolutePath()));
        }

        /**
         * Pipe 4
         * Conditional process to split a file into train, dev and/or test sets
         */
        void pip4(Metadata metadata) {
            if (this.fileSplitter != null) {
                this.fileSplitter.split(metadata);
            }
        }
    }

    class LineInfo {
        private final long skip;
        private final int length;

        LineInfo(long skip, int length) {
            this.skip = skip;
            this.length = length;
        }

        long getSkip() {
            return skip;
        }

        public int getLength() {
            return length;
        }
    }

    class PipeTask implements Runnable {
        private final File job;

        PipeTask(File job) {
            this.job = job;
        }

        @Override
        public void run() {
            log.info("{} data processing started", job.getName());
            Pipes pipes = new Pipes(job, FilenameUtils.getBaseName(job.getName()), new FileSplitter(writingBatchSize));
            try {
                /**
                 * ------+   +-------+   +-------+   +-------+
                 * pipe 1+-> + pipe 2+-> + pipe 3+-> + pipe 4+
                 * ------+   +-------+   +-------+   +-------+
                 */
                pipes.pip4(pipes.pipe3(pipes.pipe2(pipes.pipe1())));
                log.info("{} data processing finished", job.getName());
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
    }
}
