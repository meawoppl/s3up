package s3up;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.*;
import java.security.MessageDigest;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import javax.xml.bind.DatatypeConverter;

public class Upload {
    private final int threads;
    private final int blockSize;
    private BiConsumer<Long, Long> progress;

    private Upload(int threads, int blockSize) {
        this.threads = threads;
        this.blockSize = blockSize;
        progress = null;
    }

    public void setProgress(BiConsumer<Long, Long> progress) {
        this.progress = progress;
    }

    public static Upload defaultSettings() {
        return new Upload(Runtime.getRuntime().availableProcessors() * 2, 5 * 1024 * 1024);
    }

    public void uploadFile(File file, String bucketName) {
        String keyName = file.getName();

        AmazonS3 amazonS3 =
                AmazonS3ClientBuilder.standard()
                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                        .build();

        long fileSize = file.length();
        int chunkCount = (int) Math.ceil((1.0) * fileSize / blockSize);

        InputStream inStream = Util.doesNotThrow(() -> new FileInputStream(file));

        AtomicLong bytesSent = new AtomicLong(0);

        try (MultiPartClosable mpc = new MultiPartClosable(amazonS3, bucketName, keyName); ) {
            mpc.init();
            updateProgress(0, fileSize);
            ForkJoinPool customThreadPool = new ForkJoinPool(threads);

            final ForkJoinTask<?> task =
                    customThreadPool.submit(
                            () -> {
                                IntStream.range(0, chunkCount)
                                        .mapToObj(
                                                chunkId -> {
                                                    byte[] chunk =
                                                            Util.doesNotThrow(
                                                                    () ->
                                                                            inStream.readNBytes(
                                                                                    blockSize));

                                                    return mpc.nextPart()
                                                            .withMD5Digest(md5(chunk))
                                                            .withInputStream(
                                                                    new ByteArrayInputStream(chunk))
                                                            .withPartSize(chunk.length)
                                                            .withLastPart(
                                                                    chunkId == chunkCount - 1);
                                                })
                                        .parallel()
                                        .peek(mpc::sendRequest)
                                        .forEach(
                                                req -> {
                                                    updateProgress(
                                                            bytesSent.addAndGet(req.getPartSize()),
                                                            fileSize);
                                                });
                            });

            task.join();
            mpc.complete();
            updateProgress(fileSize, fileSize);
        }
    }

    private void updateProgress(long completed, long total) {
        if (progress != null) {
            progress.accept(completed, total);
        }
    }

    public static String md5(byte[] data) {
        MessageDigest md = Util.doesNotThrow(() -> MessageDigest.getInstance("MD5"));
        md.update(data);
        return DatatypeConverter.printBase64Binary(md.digest());
    }
}
