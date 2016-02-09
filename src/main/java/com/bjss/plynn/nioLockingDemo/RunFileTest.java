package com.bjss.plynn.nioLockingDemo;

import com.bjss.plynn.nioLockingUtils.ExecutionPersister;
import com.bjss.plynn.nioLockingUtils.InputFileClaimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

/*
 * Demo application for proposed file claimer package.  This is meant to be used so that multiple
 * cron jobs, or even cron jobs on different systems, can try to load the same input file and
 * simply ignore the file if it's already being worked on.  Requires two instances for testing.
 */

public class RunFileTest
{
    static Logger myLog;

    public static void main(String[] args)
    {
        Logger myLog = LoggerFactory.getLogger(RunFileTest.class);
        if (args.length < 1)
        {
            myLog.error("Must enter a file name; there are no arguments.");
            System.exit(1);
        }

        // Variables that need to persist outside of the try blocks.
        Boolean persisterGotFile;
        InputFileClaimer claim;
        ExecutionPersister persister;
        try
        {
            // Use the tool to claim an input file.
            claim = new InputFileClaimer();
            claim.getLockFile(args[0]);
            if (claim.wasDenied())
            {
                myLog.info(" The lock was in use.  The file is already being loaded by another process.");
                return;
            }

            // Pretend to do processing here.  Wait for user input to simulate
            // long-running processing.
            System.out.println(" You now have the file lock.  Claim it in the database....");

            // We have physical possession of the file.  Update its database status unless it was already completed.
            persister = new ExecutionPersister(claim.getFileName());
            System.out.println(" You now have the file claim.  Please open the file and process it.");

            persisterGotFile = persister.claimFile();

        } catch (IOException| SQLException | ClassNotFoundException ex)
        {
            flagApplicationFailureForAttention(ex);
            return;
        }
        if (! persisterGotFile)
        {
            System.out.println(" The file has already been processed (" + claim.getFileName() + ").  It is a duplicate and should be resolved");
            return;
        }

        // Pretend to process it and complete it in the database.  Wait for user input to simulate long-running
        // processing. Note that this is not in the try-catch blocks that deal with locking and persistence.
        countInputLines(claim.getFullPathName());

        try {
            persister.completeFile();
            // Use the tool to release the claim on the input file.
            claim.releaseLockFile();

        } catch (IOException | SQLException ex)
        {
            flagApplicationFailureForAttention(ex);
        }
        System.out.println(" The file claim and lock were released.");

    }

    /**
     * Method to alert designated operations persons that an unexpected appliaction error
     * has occurred.  This must be resolved at highest priority as it may have stopped
     * a file from being processed
     * @param ex The exception thrown in the coordination
     */
    private static void flagApplicationFailureForAttention(Exception ex)
    {
        // Need a PrintStream to get the stack trace into the logger.
        Logger myLog = LoggerFactory.getLogger(ExecutionPersister.class);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        ex.printStackTrace(ps);

        myLog.error("Exception occurred in the file locking / claiming code.  This must be investigated.  Cause follows:");
        myLog.error(baos.toString());
    }

    // Silly pretend processing
    private static void countInputLines(String fileName)
    {
        int counter = 0;
        Path inFilePath = FileSystems.getDefault().getPath(fileName);
        try (BufferedReader fis = Files.newBufferedReader(inFilePath, Charset.defaultCharset()))
        {
            while (fis.readLine() != null)
            {
                counter += 1;
            }
            // In a try-with-resources, you don't have to close the resource.  How strange!
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(" The file has " + counter + " lines.");

        System.out.print(" Press Enter to continue and release the claim");
        try
        {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

/*
 *      TEST CONDITIONS
 *      Failures:
 *      Input file does not exist
 *      Input file is not readable
 *      Can't create lock file: Input directory unwriteable
 *      Can't create lock file: Lock file exists and is unwriteable
 *      Database record already exists
 *
 *      Successes (require removing the database record first):
 *      Run once.  Lock claimed and released.
 *      Run, do not press enter.  Run another process.  It should note that the file is in use.
 *      Run, do not press enter, use the simpler tester to do a lock with wait and ensure that
 *          it continues when the process is aborted or concluded cleanly.
 *
 *      **** There is a very curious condition that the lock file can be deleted while
 *      **** another process still thinks it has a valid channel and locks and unlocks.
 *      Use simpler tester to open the lock file and then test to see if the lock file delete
 *          deletes the file, fails miserably, or leaves  the file in place.  It deletes the
 *          lock file but the other process will blithely lock it.  Should I not delete the lock
 *          file and let the move-to-archive step take care of it?  Two processes can now thin
 *          they have a lock on the file at the same time; the lock id is the same.
 */

