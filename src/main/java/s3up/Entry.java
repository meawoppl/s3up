package s3up;

import java.io.*;
import java.nio.file.Paths;
import java.util.function.Consumer;
import me.tongfei.progressbar.ProgressBar;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Entry {

    public static final String BUCKET_FLAG = "bucket";
    public static final String DRYRUN_FLAG = "dryrun";
    public static final String DELETE_LOCAL_FLAG = "deleteLocal";
    public static final String FILENAME_FLAG = "filename";

    public static void main(String[] args) {
        main(args, System::exit);
    }

    public static void main(String[] args, Consumer<Integer> onExit) {
        ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("S3UploadFaster");

        argumentParser.addArgument("--" + BUCKET_FLAG).help("The name of the bucket to upload to");
        argumentParser
                .addArgument("--" + DRYRUN_FLAG)
                .type(Boolean.class)
                .action(Arguments.storeTrue())
                .setDefault(Boolean.FALSE)
                .help("Only confirm that the file to be uploaded exist and exit.");
        argumentParser
                .addArgument("--" + DELETE_LOCAL_FLAG)
                .type(Boolean.class)
                .action(Arguments.storeTrue())
                .setDefault(Boolean.FALSE)
                .help("Delete file after uploading");
        argumentParser.addArgument(FILENAME_FLAG).help("The file to upload");
        Namespace namespace;

        try {
            namespace = argumentParser.parseArgs(args);
        } catch (ArgumentParserException e) {
            e.printStackTrace();
            argumentParser.printHelp();
            onExit.accept(1);
            return;
        }

        File file = Paths.get(namespace.getString(FILENAME_FLAG)).toFile();
        String bucketName = namespace.getString(BUCKET_FLAG);

        boolean dryRun = (Boolean) namespace.get(DRYRUN_FLAG);
        boolean delete = (Boolean) namespace.get(DELETE_LOCAL_FLAG);

        if (!file.exists()) {
            System.err.printf("File does not exist: %s\n", file);
            onExit.accept(1);
            return;
        }

        if (dryRun) {
            System.out.printf("Dryrun, not uploading: %s\n", file);
            onExit.accept(0);
            return;
        }

        final Upload upload = Upload.defaultSettings();
        try (ProgressBar pb = new ProgressBar("Upload", file.length())) {
            upload.setProgress(
                    (done, total) -> {
                        pb.stepTo(done);
                        pb.maxHint(total);
                    });
            upload.uploadFile(file, bucketName);
        }

        if (delete) {
            boolean succcess = file.delete();
            if (!succcess) {
                System.err.printf("Could not delete local file: %s\n", file);
                onExit.accept(1);
            } else {
                System.out.println("Removed local file.");
                onExit.accept(0);
            }
        }
    }
}
