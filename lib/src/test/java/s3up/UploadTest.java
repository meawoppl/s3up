package s3up;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class UploadTest {
    public final String TEST_BUCKET_NAME = "chia-test-7923-9832";

    @Test
    public void testUploadReadme(){
        final Path path = Paths.get("../README.md");
        Upload.uploadFile(path.toFile(), TEST_BUCKET_NAME);
    }

    @Test
    public void testUploadBig(@TempDir Path temp) throws IOException {

        File f = temp.resolve("placeholder.dat").toFile();

        try (FileOutputStream outputStream = new FileOutputStream(f)) {
            for (int i = 0; i < 1000; i++) {
                outputStream.write(new byte[1024*1024]);
            }
        }

        Upload.uploadFile(f, TEST_BUCKET_NAME);
    }

}