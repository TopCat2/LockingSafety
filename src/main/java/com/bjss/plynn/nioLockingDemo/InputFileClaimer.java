package com.bjss.plynn.nioLockingDemo;

/*
 *  Class uses advisory locking on a separate .lock file to
 *  coordinate the use of an input file among cooperating
 *  processes.  The input file itself is not touched.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class InputFileClaimer {
    public static final String LOCKFILEEXTENSION = ".lock"; // Append to file name to get lock file name
    Boolean initialized;
    Logger myLog;
    String errMsg;
    FileChannel claimFc;
    FileLock claimFl;
    Path inputFp;

    public enum Outcome {SUCCESS, DENIED, ERROR}

    InputFileClaimer() {
        initialized = false;
        myLog = LoggerFactory.getLogger(InputFileClaimer.class);
    }

    // Create or open the .lock file and attempt to claim the "lock."
    public Outcome getLockFile(String fileName) {
        // Error on already in use
        if (initialized) {
            myLog.error("PID {}, file {}: getClaim invoked on already initialized object",
                    ManagementFactory.getRuntimeMXBean().getName(), fileName);
            errMsg = "getLockFile() invoked on already initialized object";
            return Outcome.ERROR;
        }
        // Error on can't read file
        inputFp = FileSystems.getDefault().getPath(fileName).toAbsolutePath();
        if (!Files.isRegularFile(inputFp) || !Files.isReadable(inputFp)) {
            myLog.error("PID {}, file {}: File {} does not exist or is not readable in getClaim", new String[]
                    {ManagementFactory.getRuntimeMXBean().getName(), inputFp.toString(), inputFp.toString()});
            errMsg = "File does not exist or is not readable in getClaim";
            return Outcome.ERROR;
        }

        try {
            claimFc = new RandomAccessFile(inputFp.toString() + LOCKFILEEXTENSION, "rw").getChannel();
            myLog.info("Got the FileChannel: " + claimFc);
        } catch (Exception e) {
            myLog.error("PID {}, file {}: Can't open the FileChannel.",
                    ManagementFactory.getRuntimeMXBean().getName(), inputFp.toString());
            myLog.error(e.toString());
            errMsg = "getClaim cannot open the FileChannel. " + e.toString();
            return Outcome.ERROR;
        }

        try {
            claimFl = claimFc.tryLock();
        } catch (Exception e) {
            myLog.error("PID {}, file {}: Exception trying to lock the file.",
                    ManagementFactory.getRuntimeMXBean().getName(), inputFp.toString());
            errMsg = "Exception in getClaim locking the file. " + e.toString();
            return Outcome.ERROR;
        }
        initialized = true;
        if (claimFl == null) {
            return Outcome.DENIED;
        } else {
            return Outcome.SUCCESS;
        }
    }

    // "Release the lock."  Implementation is loosely defined.  It could
    // simply release the lock, close the channel to the lock file, or
    // even delete the lock file.

    public Outcome releaseLockFile(String fileName) {
        // Error on already in use
        if (!initialized) {
            myLog.error("PID {}, file {}: releaseLockFile invoked on uninitialized object",
                    ManagementFactory.getRuntimeMXBean().getName(), fileName);
            errMsg = "getLockFile() invoked on uninitialized object";
            return Outcome.ERROR;
        }

        try {
            claimFl.release();
            claimFc.close();
                /**** Need to test how the delete works if someone else has the file open. ****/
                /**** Use the SimpleFileRunTest on the input file name to control this. ****/
        } catch (IOException e) {
            myLog.error("PID {}, file {}: Failed releasing the lock/",
                    ManagementFactory.getRuntimeMXBean().getName(), inputFp.toString());
            myLog.error(e.toString());
            errMsg = "Failed releasing lock on " + e.toString() + ".  " + e.toString();
            return Outcome.ERROR;
        }

        /*
         * There is an interesting file with deleting the lock file.  Any process that still has a channel thinks
         * that it can get the lock on the old version of the file.  The creation of a new lock file makes future
         * processes unrelated to the ones attached to the old version.
         */
        try {
            Files.delete(FileSystems.getDefault().getPath(inputFp.toString() + LOCKFILEEXTENSION));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Outcome.SUCCESS;
    }

    public String getPathName() {
        return inputFp.toString();
    }

    public String getErrMsg() {
        return errMsg;
    }
}
