package com.mars.fuzznnrl.data;

import ch.qos.logback.classic.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileSplitter {
    private static Logger log = (Logger) LoggerFactory.getLogger(FileSplitter.class);
    private int writingBatchSize;
    private final ISplittingStrategy splittingStrategy;
    private boolean hasWrittenBefore_train, hasWrittenBefore_dev, hasWrittenBefore_test;

    /**
     * Creates a file splitter using the default splitting strategy
     * @param writingBatchSize
     */
    public FileSplitter(int writingBatchSize) {
        this(writingBatchSize, new ISplittingStrategy() {
        });
    }

    /**
     * Creates a file splitter using the passed splitting strategy
     *
     * @param writingBatchSize
     * @param splittingStrategy
     */
    public FileSplitter(int writingBatchSize, ISplittingStrategy splittingStrategy) {
        this.writingBatchSize = writingBatchSize;
        this.splittingStrategy = splittingStrategy;
    }

    public ISplittingStrategy getSplittingStrategy() {
        return splittingStrategy;
    }

    public void split(Metadata metadata) {
        String filepath = metadata.getFilepath();
        log.info("{} splitting started", FilenameUtils.getBaseName(filepath));
        long train_size, dev_size, test_size;
        SplitInfo splitInfo = getSplitInfo(metadata);
        long numLines = metadata.getNumLines();
        train_size = (long) Math.floor(splitInfo.getTrain() * numLines);
        dev_size = (long) Math.floor(splitInfo.getDev() * numLines);
        test_size = (long) Math.floor(splitInfo.getTest() * numLines);
        train_size += numLines - (train_size + dev_size + test_size); //adds the surplus to the training set

        try (LineIterator lineIterator = FileUtils.lineIterator(new File(filepath))) {
            File trainFile = new File(String.format("%s%s_train.%s", metadata.getProcessedStorageDir(),
                    FilenameUtils.getBaseName(filepath), FilenameUtils.getExtension(filepath)));
            File devFile = new File(String.format("%s%s_dev.%s", metadata.getProcessedStorageDir(),
                    FilenameUtils.getBaseName(filepath), FilenameUtils.getExtension(filepath)));
            File testFile = new File(String.format("%s%s_test.%s", metadata.getProcessedStorageDir(),
                    FilenameUtils.getBaseName(filepath), FilenameUtils.getExtension(filepath)));

            List<String> trainLines = new ArrayList<>();
            List<String> devLines = new ArrayList<>();
            List<String> testLines = new ArrayList<>();
            long count = 1;
            while (lineIterator.hasNext()) {
                String next = lineIterator.next();
                if (count <= train_size) {
                    if ((count + 1) > train_size)
                        next = StringUtils.removeEnd(next, System.lineSeparator());
                    else
                        next += System.lineSeparator();
                    trainLines.add(next);
                } else if (count <= (train_size + dev_size)) {
                    if ((count + 1) > (train_size + dev_size))
                        next = StringUtils.removeEnd(next, System.lineSeparator());
                    else
                        next += System.lineSeparator();
                    devLines.add(next);
                } else {
                    if ((count + 1) > metadata.getNumLines())
                        next = StringUtils.removeEnd(next, System.lineSeparator());
                    else
                        next += System.lineSeparator();
                    testLines.add(next);
                }

                if (count % writingBatchSize == 0) {
                    writeToFiles(trainFile, devFile, testFile, trainLines, devLines, testLines);
                }
                count++;
            }

            //takes care of data below batch size or left after batching
            writeToFiles(trainFile, devFile, testFile, trainLines, devLines, testLines);
            log.info("{} splitting finished", FilenameUtils.getBaseName(filepath));
        } catch (IOException e) {
            log.error("Could not split file {} : {}", filepath, e.getMessage());
        }
    }

    /**
     * Writes the selected lines of the shuffled file to their respective partition file
     *
     * @param trainFile
     * @param devFile
     * @param testFile
     * @param trainLines
     * @param devLines
     * @param testLines
     * @throws IOException
     */
    private void writeToFiles(File trainFile, File devFile, File testFile, List<String> trainLines,
                              List<String> devLines, List<String> testLines) throws IOException {
        //Train set
        FileUtils.writeLines(trainFile, trainLines, "", true);
        if (trainLines.size() > 0) hasWrittenBefore_train = true;
        trainLines.clear();

        //Hold-out cross validation or dev set
        FileUtils.writeLines(devFile, devLines, "", true);
        if (trainLines.size() > 0) hasWrittenBefore_dev = true;
        devLines.clear();

        //Test set
        FileUtils.writeLines(testFile, testLines, "", true);
        if (trainLines.size() > 0) hasWrittenBefore_test = true;
        testLines.clear();
    }

    private SplitInfo getSplitInfo(Metadata metadata) {
        SplitInfo splitInfo;
        int thousand = 1000;
        int tenThousand = 10000;
        int oneMillion = 1000000;
        if (metadata.getNumLines() >= tenThousand && metadata.getNumLines() < oneMillion)
            splitInfo = this.splittingStrategy.leqTenThousand();
        else if (metadata.getNumLines() >= oneMillion) splitInfo = this.splittingStrategy.grtEqMillion();
        else splitInfo = this.splittingStrategy.leqThousand();
        return splitInfo;
    }
}
