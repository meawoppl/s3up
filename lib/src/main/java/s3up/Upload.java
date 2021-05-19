package s3up;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import me.tongfei.progressbar.ProgressBar;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Upload {
    public static void uploadFile(File file, String bucketName) {
        int blockSize = 5 * 1024 * 1024;
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


        List<PartETag> etags;
        try(ProgressBar pb = new ProgressBar("Upload", chunkCount)) {
            pb.setExtraMessage("Initializing");
            InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(bucketName, file.getName());
            InitiateMultipartUploadResult initRes = amazonS3.initiateMultipartUpload(initReq);
            String uploadID = initRes.getUploadId();

            try {
                pb.setExtraMessage("Chunks");
                etags = IntStream.range(0, chunkCount).mapToObj(chunkId -> {
                    byte[] chunk;
                    try {
                        chunk = inStream.readNBytes(blockSize);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    System.out.println(Arrays.toString(chunk));
                    String md5 = md5(chunk);
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(chunk);

                    UploadPartRequest req = new UploadPartRequest()
                            .withBucketName(bucketName)
                            .withKey(keyName)
                            .withPartNumber(chunkId + 1) // Chunks are 1 based....
                            .withUploadId(uploadID)
                            .withMD5Digest(md5)
                            .withInputStream(inputStream)
                            .withPartSize(chunk.length)
                            .withLastPart(chunkId == chunkCount - 1);

                    return req;
                }).parallel().map(req -> {
                    UploadPartResult resp = amazonS3.uploadPart(req);
                    return resp.getPartETag();
                }).peek(r -> pb.step()).collect(Collectors.toList());

                pb.setExtraMessage("Finalizing...");
                CompleteMultipartUploadResult completeResult = amazonS3.completeMultipartUpload(new CompleteMultipartUploadRequest()
                        .withBucketName(bucketName)
                        .withKey(keyName)
                        .withUploadId(uploadID)
                        .withPartETags(etags)
                );
            } catch (Exception e){
                amazonS3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, keyName, uploadID));
            }
        }
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
