//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.amazon.elasticmapreduce.s3distcp;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.hadoop.compression.lzo.LzopCodec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyFilesReducer extends Reducer<Text, FileInfo, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(CopyFilesReducer.class);
    private static final List<String> validCodecs = Lists.newArrayList(new String[]{"snappy", "gz", "lzo", "lzop", "gzip"});
    private Reducer<Text, FileInfo, Text, Text>.Context context;
    private SimpleExecutor transferQueue;
    private Set<FileInfo> uncommittedFiles;
    private long targetSize;
    private int bufferSize;
    private int numTransferRetries;
    private String outputCodec;
    private String outputSuffix;
    private boolean deleteOnSuccess;
    private boolean numberFiles;
    private boolean appendToLastFile;
    private boolean group;
    private Configuration conf;

    public CopyFilesReducer() {
    }

    protected void cleanup(Reducer<Text, FileInfo, Text, Text>.Context context) throws IOException, InterruptedException {
        this.transferQueue.close();
        synchronized(this.uncommittedFiles) {
            if (this.uncommittedFiles.size() > 0) {
                logger.warn("CopyFilesReducer uncommitted files " + this.uncommittedFiles.size());
                Iterator var3 = this.uncommittedFiles.iterator();

                while(var3.hasNext()) {
                    FileInfo fileInfo = (FileInfo)var3.next();
                    logger.warn("Failed to upload " + fileInfo.inputFileName);
                    context.write(fileInfo.outputFileName, fileInfo.inputFileName);
                }

                String message = String.format("Reducer task failed to copy %d files: %s etc", this.uncommittedFiles.size(), ((FileInfo)this.uncommittedFiles.iterator().next()).inputFileName);
                throw new RuntimeException(message);
            }
        }
    }

    public Configuration getConf() {
        return this.conf;
    }

    public boolean shouldDeleteOnSuccess() {
        return this.deleteOnSuccess;
    }

    protected void setup(Reducer<Text, FileInfo, Text, Text>.Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
        int queueSize = this.conf.getInt("s3DistCp.copyfiles.mapper.queueSize", 10);
        int numWorkers = this.conf.getInt("s3DistCp.copyfiles.mapper.numWorkers", 5);
        this.transferQueue = new SimpleExecutor(queueSize, numWorkers);
        this.bufferSize = this.conf.getInt("s3DistCp.copyfiles.mapper.bufferSize", 1048576);
        this.targetSize = this.conf.getLong("s3DistCp.copyfiles.reducer.targetSize", 9223372036854775807L);
        this.outputCodec = this.conf.get("s3DistCp.copyfiles.reducer.outputCodec").toLowerCase();
        this.numberFiles = this.conf.getBoolean("s3DistCp.copyfiles.reducer.numberFiles", false);
        this.uncommittedFiles = Collections.synchronizedSet(new HashSet());
        this.deleteOnSuccess = this.conf.getBoolean("s3DistCp.copyFiles.deleteFilesOnSuccess", false);
        this.numTransferRetries = this.conf.getInt("s3DistCp.copyfiles.mapper.numRetries", 10);
        this.appendToLastFile = this.conf.getBoolean("s3distcp.copyFiles.appendToLastFile", false);
        this.group = this.conf.get("s3DistCp.listfiles.groupByPattern", (String)null) != null;
    }

    public int getNumTransferRetries() {
        return this.numTransferRetries;
    }

    public int getBufferSize() {
        return this.bufferSize;
    }

    private String makeFinalPath(long fileUid, String finalDir, String groupId, String groupIndex) {
        String[] groupIds = groupId.split("/");
        groupId = groupIds[groupIds.length - 1];
        if (this.numberFiles) {
            groupId = fileUid + groupId;
        }

        String suffix;
        if (!Strings.isNullOrEmpty(this.outputSuffix)) {
            suffix = groupIndex + "." + this.outputSuffix;
        } else {
            suffix = groupIndex;
        }

        String groupIdSuffix = Utils.getSuffix(groupId);
        return !Strings.isNullOrEmpty(groupIdSuffix) && validCodecs.contains(groupIdSuffix) ? finalDir + "/" + Utils.replaceSuffix(groupId, suffix) : finalDir + "/" + Utils.appendSuffix(groupId, suffix);
    }

    private String determineOutputSuffix(List<FileInfo> fileInfos) {
        String codec;
        Iterator var3;
        FileInfo fileInfo;
        String currentSuffix;
        if (this.outputCodec.equalsIgnoreCase("keep")) {
            codec = null;
            var3 = fileInfos.iterator();

            while(var3.hasNext()) {
                fileInfo = (FileInfo)var3.next();
                currentSuffix = Utils.getSuffix(fileInfo.inputFileName.toString()).toLowerCase();
                if (!validCodecs.contains(currentSuffix)) {
                    currentSuffix = "";
                }

                if (codec == null) {
                    codec = currentSuffix;
                } else if (!codec.equals(currentSuffix)) {
                    throw new RuntimeException("Cannot keep compression scheme for input files with different compression schemes.");
                }
            }

            if (codec == null) {
                return null;
            } else {
                byte var6 = -1;
                switch(codec.hashCode()) {
                    case -898026669:
                        if (codec.equals("snappy")) {
                            var6 = 4;
                        }
                        break;
                    case 0:
                        if (codec.equals("")) {
                            var6 = 5;
                        }
                        break;
                    case 3315:
                        if (codec.equals("gz")) {
                            var6 = 0;
                        }
                        break;
                    case 107681:
                        if (codec.equals("lzo")) {
                            var6 = 2;
                        }
                        break;
                    case 3189082:
                        if (codec.equals("gzip")) {
                            var6 = 1;
                        }
                        break;
                    case 3338223:
                        if (codec.equals("lzop")) {
                            var6 = 3;
                        }
                }

                switch(var6) {
                    case 0:
                    case 1:
                        return "gz";
                    case 2:
                    case 3:
                        return "lzo";
                    case 4:
                        return "snappy";
                    case 5:
                        return "";
                    default:
                        throw new RuntimeException("Unsupported output codec: " + codec);
                }
            }
        } else if (this.outputCodec.equalsIgnoreCase("none")) {
            codec = null;
            var3 = fileInfos.iterator();

            while(var3.hasNext()) {
                fileInfo = (FileInfo)var3.next();
                currentSuffix = Utils.getSuffix(fileInfo.inputFileName.toString()).toLowerCase();
                if (codec == null) {
                    codec = currentSuffix;
                } else if (!codec.equals(currentSuffix)) {
                    return "";
                }
            }

            if (validCodecs.contains(codec)) {
                codec = "";
            }

            return codec;
        } else {
            return this.outputCodec.toLowerCase();
        }
    }

    protected void reduce(Text groupKey, Iterable<FileInfo> fileInfos, Reducer<Text, FileInfo, Text, Text>.Context context) throws IOException, InterruptedException {
        this.context = context;
        long curSize = 0L;
        int groupNum = -1;
        List<FileInfo> allFiles = new ArrayList();
        List<FileInfo> curFiles = new ArrayList();
        String groupId = Utils.cleanupColonsAndSlashes(groupKey.toString());
        Path finalPath = null;

        while(fileInfos.iterator().hasNext()) {
            allFiles.add(((FileInfo)fileInfos.iterator().next()).clone());
        }

        this.outputSuffix = this.determineOutputSuffix(allFiles);
        logger.info("Output suffix: '" + this.outputSuffix + "'");

        for(int i = 0; i < allFiles.size(); ++i) {
            FileInfo fileInfo = ((FileInfo)allFiles.get(i)).clone();
            curSize += fileInfo.fileSize.get();
            curFiles.add(fileInfo);
            Path parentDir = (new Path(fileInfo.outputFileName.toString())).getParent();
            if (finalPath == null) {
                do {
                    ++groupNum;
                    finalPath = new Path(this.makeFinalPath(fileInfo.fileUID.get(), parentDir.toString(), groupId, this.getGroupIndex(groupNum)));
                } while(CopyFilesReducerHelper.canGenerateFinalPath(this.group, finalPath, this.getConf()));

                if (this.appendToLastFile && this.group) {
                    Path tempPath = new Path(this.makeFinalPath(fileInfo.fileUID.get(), parentDir.toString(), groupId, this.getGroupIndex(groupNum - 1)));
                    if (tempPath.getFileSystem(this.getConf()).exists(tempPath)) {
                        long tempFileSize = tempPath.getFileSystem(this.getConf()).getFileStatus(tempPath).getLen();
                        if (tempFileSize + curSize < this.targetSize) {
                            curSize += tempFileSize;
                            finalPath = tempPath;
                            --groupNum;
                        }
                    }
                }
            }

            if (curSize >= this.targetSize || i == allFiles.size() - 1) {
                logger.info("finalPath:" + finalPath);
                this.executeDownloads(this, curFiles, finalPath);
                curFiles = new ArrayList();
                curSize = 0L;
                ++groupNum;
                finalPath = new Path(this.makeFinalPath(fileInfo.fileUID.get(), parentDir.toString(), groupId, this.getGroupIndex(groupNum)));
            }
        }

    }

    private String getGroupIndex(int groupNum) {
        return groupNum == 0 ? "" : Integer.toString(groupNum);
    }

    private void executeDownloads(CopyFilesReducer reducer, List<FileInfo> fileInfos, Path finalPath) {
        this.uncommittedFiles.addAll(fileInfos);
        Iterator var4 = fileInfos.iterator();

        while(var4.hasNext()) {
            FileInfo fileInfo = (FileInfo)var4.next();
            logger.info("Processing object: " + fileInfo.inputFileName.toString());
        }

        if (fileInfos.size() > 0) {
            logger.info("Processing " + fileInfos.size() + " files");
            this.transferQueue.execute(new CopyFilesRunnable(reducer, fileInfos, finalPath));
        } else {
            logger.info("No files to process");
        }

    }

    public void markFilesAsCommitted(List<FileInfo> fileInfos) {
        synchronized(this.uncommittedFiles) {
            logger.info("Marking " + fileInfos.size() + " files as committed");
            Iterator var3 = fileInfos.iterator();

            while(true) {
                if (!var3.hasNext()) {
                    this.uncommittedFiles.removeAll(fileInfos);
                    break;
                }

                FileInfo fileInfo = (FileInfo)var3.next();
                logger.info("Committing file: " + fileInfo.inputFileName);
            }
        }

        this.progress();
    }

    public InputStream decorateInputStream(InputStream inputStream, Path inputFilePath) throws IOException {
        String suffix = Utils.getSuffix(inputFilePath.getName()).toLowerCase();
        byte var5 = -1;
        switch(suffix.hashCode()) {
            case -898026669:
                if (suffix.equals("snappy")) {
                    var5 = 2;
                }
                break;
            case 3315:
                if (suffix.equals("gz")) {
                    var5 = 0;
                }
                break;
            case 107681:
                if (suffix.equals("lzo")) {
                    var5 = 4;
                }
                break;
            case 3189082:
                if (suffix.equals("gzip")) {
                    var5 = 1;
                }
                break;
            case 3338223:
                if (suffix.equals("lzop")) {
                    var5 = 3;
                }
        }

        switch(var5) {
            case 0:
            case 1:
                //FileSystem inputFs = inputFilePath.getFileSystem(this.conf);
                //return new GZIPInputStream(new ByteCounterInputStream(inputStream, inputFs.getFileStatus(inputFilePath).getLen()));
            case 2:
                //SnappyCodec codec = new SnappyCodec();
                //codec.setConf(this.getConf());
                //return codec.createInputStream(inputStream);
            case 3:
            case 4:
                //LzopCodec codec = new LzopCodec();
                //codec.setConf(this.getConf());
                //return codec.createInputStream(inputStream);
            default:
                return inputStream;
        }
    }

    public InputStream openInputStream(Path inputFilePath) throws IOException {
        FileSystem inputFs = inputFilePath.getFileSystem(this.conf);
        return inputFs.open(inputFilePath);
    }

    public OutputStream decorateOutputStream(OutputStream outputStream, Path outputFilePath) throws IOException {
        String suffix = Utils.getSuffix(outputFilePath.getName()).toLowerCase();
        byte var5 = -1;
        switch(suffix.hashCode()) {
            case -898026669:
                if (suffix.equals("snappy")) {
                    var5 = 4;
                }
                break;
            case 3315:
                if (suffix.equals("gz")) {
                    var5 = 0;
                }
                break;
            case 107681:
                if (suffix.equals("lzo")) {
                    var5 = 2;
                }
                break;
            case 3189082:
                if (suffix.equals("gzip")) {
                    var5 = 1;
                }
                break;
            case 3338223:
                if (suffix.equals("lzop")) {
                    var5 = 3;
                }
        }

        switch(var5) {
            case 0:
            case 1:
                //return new GZIPOutputStream(outputStream);
            case 2:
            case 3:
                //LzopCodec codec = new LzopCodec();
                //codec.setConf(this.getConf());
                //return codec.createOutputStream(outputStream);
            case 4:
                //SnappyCodec codec = new SnappyCodec();
                //codec.setConf(this.getConf());
                //return codec.createOutputStream(outputStream);
            default:
                return outputStream;
        }
    }

    public OutputStream openOutputStream(Path outputFilePath) throws IOException {
        FileSystem outputFs = outputFilePath.getFileSystem(this.conf);
        FSDataOutputStream outputStream;
        if (!Utils.isS3Scheme(outputFilePath.getFileSystem(this.conf).getUri().getScheme()) && outputFs.exists(outputFilePath) && this.appendToLastFile) {
            outputStream = outputFs.append(outputFilePath);
        } else {
            outputStream = outputFs.create(outputFilePath);
        }

        return outputStream;
    }

    public void progress() {
        this.context.progress();
    }
}

