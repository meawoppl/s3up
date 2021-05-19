package s3up;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UploadTest {
    public final String TEST_BUCKET_NAME = "chia-test-7923-9832";

    @Test
    public void testUploadReadme() {
        final Path path = Paths.get("README.md");
        Upload.defaultSettings().uploadFile(path.toFile(), TEST_BUCKET_NAME);
    }

    @Test
    public void testCallback() {
        final Path path = Paths.get("README.md");
        AtomicBoolean min = new AtomicBoolean(false);
        AtomicBoolean max = new AtomicBoolean(false);

        final Upload upload = Upload.defaultSettings();
        upload.setProgress(
                (done, total) -> {
                    if (done == 0) {
                        min.set(true);
                    }

                    if (done.equals(total)) {
                        max.set(true);
                    }
                });
        upload.uploadFile(path.toFile(), TEST_BUCKET_NAME);

        Assertions.assertTrue(min.get());
        Assertions.assertTrue(max.get());
    }

    private void populateFile(Path path, int nMB) throws IOException {
        File f = path.toFile();

        try (FileOutputStream outputStream = new FileOutputStream(f)) {
            for (int i = 0; i < nMB; i++) {
                outputStream.write(new byte[1024 * 1024]);
            }
        }
    }

    @Test
    public void testUploadBig(@TempDir Path temp) throws IOException {
        int megabytes = 1000;
        Path path = temp.resolve("placeholder.dat");
        populateFile(path, megabytes);

        Instant start = Instant.now();
        Upload.defaultSettings().uploadFile(path.toFile(), TEST_BUCKET_NAME);
        Instant end = Instant.now();

        final Duration between = Duration.between(start, end);
        double seconds = between.getSeconds() + between.getNano() * 1e-9;

        // Megabit per second
        System.out.printf("%f Mb/s\n", 8 * megabytes / seconds);
    }
}
