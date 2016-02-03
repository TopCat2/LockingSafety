package com.bjss.plynn.nioLockingDemo;

/*
 *  Class uses advisory locking on a separate .lock file to
 *  coordinate the use of an input file among cooperating
 *  processes.  The input file itself is not touched.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class InputFileClaimer
{
    public static final String LOCKFILEEXTENSION = ".lock"; // Append to file name to get lock file name
    Boolean initialized;
    Logger myLog;
    String errMsg;
    FileChannel claimFc;
    FileLock claimFl;
    Path inputFp;
    Boolean lockGranted;

    InputFileClaimer()
    {
        initialized = false;
        myLog = LoggerFactory.getLogger(InputFileClaimer.class);
    }

    // Create or open the .lock file and attempt to claim the "lock."
    public void getLockFile(String fileName) throws IOException
    {
        // Error on already in use
        if (initialized)
        {
            throw new IllegalStateException("InputFileClaimer.getLockFile() invoken on already initialized object");
        }

        // Throw exception on not a file...
        inputFp = FileSystems.getDefault().getPath(fileName).toAbsolutePath();
        if (!Files.isRegularFile(inputFp))
        {
            throw new FileNotFoundException(inputFp.toString());
        }
        // or on can't read file
        inputFp = FileSystems.getDefault().getPath(fileName).toAbsolutePath();
        if (!Files.isReadable(inputFp))
        {
            throw new AccessDeniedException(inputFp.toString());
        }

        initialized = true;
        // Open the channel to the file or throws an exception
        claimFc = new RandomAccessFile(inputFp.toString() + LOCKFILEEXTENSION, "rw").getChannel();
        myLog.info("Got FileChannel: {} for file {}", claimFc, inputFp.toString());

        // Try to claim the lock
        claimFl = claimFc.tryLock();

        // Set status based on whether or not the lock was available.
        if (claimFl == null)
        {
            myLog.info("Did not get nio lock for file {}", inputFp.toString());
            lockGranted = false;
        } else
        {
            myLog.info("Got nio lock: {} for file {}", claimFl, inputFp.toString());
            lockGranted = true;
        }
    }

    // "Release the lock."  Implementation is loosely defined.  It could
    // simply release the lock, close the channel to the lock file, or
    // even delete the lock file.

    public void releaseLockFile() throws IOException
    {
        // Error on already in use
        if (!initialized)
        {
            throw new IllegalStateException("InputFileClaimer.releaseLockFile() invoked on uninitialized object");
        }

        claimFl.release();
        myLog.info("Released nio lock: {} for file {}", claimFl, inputFp.toString());
        claimFc.close();

        /*
         * There is an interesting file with deleting the lock file.  Any process that still has a channel thinks
         * that it can get the lock on the old version of the file.  The creation of a new lock file makes future
         * processes unrelated to the ones attached to the old version.
         */
        try
        {
            Files.delete(FileSystems.getDefault().getPath(inputFp.toString() + LOCKFILEEXTENSION));
        } catch (IOException e) {
            myLog.info("Failed to delete lock file.  Exception is {}", e.toString());
        }
    }

    public String getPathName()
    {
        return inputFp.toString();
    }

    public String getFileName()
    {
        Path nameOnly = inputFp.getFileName();
        String name = nameOnly.toString();
        return name;
    }

    public String getErrMsg()
    {
        return errMsg;
    }

    public Boolean wasDenied()
    {
        return !lockGranted;
    }

    public Boolean wasGranted()
    {
        return lockGranted;
    }
}
