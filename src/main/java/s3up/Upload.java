package s3up;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import me.tongfei.progressbar.ProgressBar;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.IntStream;

public class Upload {
    private final int threads;
    private final int blockSize;

    private Upload(int threads, int blockSize){
        this.threads = threads;
        this.blockSize = blockSize;
    }

    public static Upload defaultSettings(){
        return new Upload(Runtime.getRuntime().availableProcessors()*2, 5 * 1024 * 1024);
    }

    public void uploadFile(File file, String bucketName) {
        String keyName = file.getName();

        AmazonS3 amazonS3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();

        long fileSize = file.length();
        int chunkCount = (int) Math.ceil((1.0) * fileSize / blockSize);

        InputStream inStream;
        try {
            inStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        Instant start = Instant.now();

        try(
                ProgressBar pb = new ProgressBar("Upload", chunkCount);
                MultiPartClosable mpc = new MultiPartClosable(amazonS3, bucketName, keyName);
        ) {
            pb.setExtraMessage("Initializing");
            mpc.init();

            pb.setExtraMessage("Chunks");
            ForkJoinPool customThreadPool = new ForkJoinPool(threads);

            final ForkJoinTask<?> task = customThreadPool.submit(() -> {
                IntStream.range(0, chunkCount).mapToObj(chunkId -> {
                    byte[] chunk;
                    try {
                        chunk = inStream.readNBytes(blockSize);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    String md5 = md5(chunk);
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(chunk);

                    return mpc.nextPart()
                            .withMD5Digest(md5)
                            .withInputStream(inputStream)
                            .withPartSize(chunk.length)
                            .withLastPart(chunkId == chunkCount - 1);
                }).parallel().forEach(req -> {
                    mpc.sendRequest(req);
                    pb.step();
                });
            });

            task.join();
            pb.setExtraMessage("Finalizing...");
            mpc.complete();
        }

        Instant end = Instant.now();

        final Duration between = Duration.between(start, end);

        double seconds =  between.getSeconds() + between.getNano() * 1e-9;

        double megs = fileSize / 1e6;

        System.out.println(megs/seconds);
    }

    public static String md5(byte[] data) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("No MD%?!?!?!");
        }
        md.update(data);

        return DatatypeConverter.printBase64Binary(md.digest());
    }
}
