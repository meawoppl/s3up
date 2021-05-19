package s3up;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MultiPartClosable implements AutoCloseable{
    private final AmazonS3 amazonS3;
    private final String bucketName;
    private final String keyName;
    private final AtomicInteger partID;
    private final Map<Integer, PartETag> tags;

    private String uploadID;

    public MultiPartClosable(AmazonS3 amazonS3, String bucketName, String keyName) {
        this.amazonS3 = amazonS3;
        this.bucketName = bucketName;
        this.keyName = keyName;
        partID = new AtomicInteger(1); // Chunks are 1 based....
        tags = new ConcurrentHashMap<>();
    }

    public void init(){
        InitiateMultipartUploadRequest initReq = new InitiateMultipartUploadRequest(bucketName, keyName);
        InitiateMultipartUploadResult initRes = amazonS3.initiateMultipartUpload(initReq);
        uploadID = initRes.getUploadId();
    }


    public UploadPartRequest nextPart(){
        return new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(keyName)
                .withPartNumber(partID.getAndIncrement())
                .withUploadId(uploadID);

    }

    public void sendRequest(UploadPartRequest request){
        UploadPartResult resp = amazonS3.uploadPart(request);
        tags.put(resp.getPartNumber(), resp.getPartETag());
    }

    public void complete(){
        List<PartETag> sorted = tags.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.toList());
        amazonS3.completeMultipartUpload(new CompleteMultipartUploadRequest()
                .withBucketName(bucketName)
                .withKey(keyName)
                .withUploadId(uploadID)
                .withPartETags(sorted)
        );

        uploadID = null;
    }


    @Override
    public void close() {
        if(uploadID != null){
            amazonS3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, keyName, uploadID));
        }
    }
}
