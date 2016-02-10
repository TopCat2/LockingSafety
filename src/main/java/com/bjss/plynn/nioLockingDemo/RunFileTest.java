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
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/*
 * Demo application for proposed file claimer package.  This is meant to be used so that multiple
 * cron jobs, or even cron jobs on different systems, can try to load the same input file and
 * simply ignore the file if it's already being worked on.  Requires two instances for testing.
 */

public class RunFileTest
{
    static Logger myLog;
    static final String WRAPPERPROPERTIESFILE = "wrapper.properties";

    // Configuration parameters read from file
    static String DIRECTORY_COMPLETED;
    static String DIRECTORY_REJECTED;
    static String DIRECTORY_FAILED;
    static int MAX_TRIES;

    public static void main(String[] args)
    {
        myLog = LoggerFactory.getLogger(RunFileTest.class);

        InputFileClaimer claim;
        ExecutionPersister persister;
        Integer recordsProcessed;
        String fullFilePath;

        try
        {
            if (args.length < 1)
            {
                myLog.error("Must enter a file name; there are no arguments.");
                // this is mostly in a throw to allow refactoring the setup into a separate method.
                throw new IllegalArgumentException("Application executed but no file name entered");
            }

            // Read properties file wrapper.properties
            readPropertiesFile();

            // Use the tool to claim an input file.
            claim = new InputFileClaimer();
            claim.getLockFile(args[0]);
            if (claim.wasDenied())
            {
                myLog.info(" The lock on " + args[0] + " was in use.  The file is already being loaded by another process.  No need to continue.");
                return;
            }
            fullFilePath = claim.getFullPathName();

            myLog.debug("Got the file lock.  Claim it in the database....");

            // We have physical possession of the file.  Update its database status unless it was already completed.
            persister = new ExecutionPersister(claim.getFileName());
            // Test for file should no longer be run
            if (! persister.claimFile())
            {
                myLog.error(" The file has already been processed or canceled (" + fullFilePath + ").  It is a duplicate and should be resolved");
// Notify operations here?
                claim.releaseLockFile();
                moveFileToFailed(fullFilePath);
                return;
            }

            // Test for maximum permissible tries exceeded.  Need to test here
            // in case the application has repeatedly crashed without marking the file.
            if (persister.getPreviousTries() > MAX_TRIES)
            {
                flagFailureForAttention("The file has been retried too many times (" + persister.getPreviousTries()
                    + ").  File " + fullFilePath + " will be moved to the failure directory.  This must be investigated.");
                moveFileToFailed(fullFilePath);
                persister.failFile();
                claim.releaseLockFile();
                return;
            }

            // Validate the file.
            myLog.debug("Got the file claim.  Now validate and process it.");
            Boolean rejected = ! validateInputFile(claim.getFullPathName());
            if (rejected)
            {
                moveFileToRejected(fullFilePath);
                persister.rejectFile();
                flagFileRejectForAttention("The file failed format validation.  Move the file to the REJECTED directory and alert customer support.");
            } else
            {
                // Process the file and complete it in the database.
                recordsProcessed = loadInputFile(claim.getFullPathName());
                myLog.info("Loaded " + recordsProcessed + " records from file " + claim.getFileName());
                moveFileToCompleted(fullFilePath);
                persister.completeFile(recordsProcessed);
            }
            claim.releaseLockFile();
        } catch (Exception ex)
        {
            flagFailureForAttention(ex);
        }

    }

    private static void moveFile(String fullFilePath, String targetDirectory) throws IOException
    {
        Path sourceFile = FileSystems.getDefault().getPath(fullFilePath);
        Path targetLoc = FileSystems.getDefault().getPath(targetDirectory, sourceFile.getFileName().toString());
        Files.move(sourceFile, targetLoc);
    }

    private static void moveFileToCompleted(String fullFilePath) throws IOException
    {
        moveFile(fullFilePath, DIRECTORY_COMPLETED);
    }

    private static void moveFileToRejected(String fullFilePath) throws IOException
    {
        moveFile(fullFilePath, DIRECTORY_REJECTED);
    }

   private static void moveFileToFailed(String fullFilePath) throws IOException
    {
        moveFile(fullFilePath, DIRECTORY_FAILED);
    }

    /**
     * Method to alert designated operations persons that an unexpected application error
     * has occurred.  This must be resolved at highest priority as it may have stopped
     * a file from being processed
     * @param ex The exception thrown in the coordination
     */
    private static void flagFailureForAttention(Exception ex)
    {
        myLog.error("Unexpected exception occurred in the loading application or utility code.  This must be investigated.  Cause follows:");
        // Need a PrintStream to get the stack trace into the logger.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        ex.printStackTrace(ps);
        myLog.error(baos.toString());

        myLog.info("Dummy alert to Operations");
//  Send the email or other notification to Operations
    }

    private static void flagFailureForAttention(String message)
    {

        myLog.error(message);
        myLog.info("Dummy alert to Operations");
//  Send the email or other notification to Operations
    }

    private static void flagFileRejectForAttention(String message)
    {
//  Send the email or other notification to customer service
        myLog.warn(message);
        myLog.info("Dummy alert to Customer Service");
    }

    static void readPropertiesFile() throws IOException
    {
        Properties prop = new Properties();
        InputStream iStream = ExecutionPersister.class.getClassLoader().getResourceAsStream(WRAPPERPROPERTIESFILE);
        if (iStream == null)
        {
            throw new FileNotFoundException("Property file " + WRAPPERPROPERTIESFILE + "was not found in the classpath");
        }
        prop.load(iStream);
        iStream.close();

        //  Database credentials
        DIRECTORY_COMPLETED  = prop.getProperty("wrapper.directory.completed", "BadValue");
        DIRECTORY_REJECTED = prop.getProperty("wrapper.directory.rejected", "BadValue");
        DIRECTORY_FAILED = prop.getProperty("wrapper.directory.failed", "BadValue");
        MAX_TRIES = Integer.parseInt(prop.getProperty("db.jdbc.url", "1"));
    }



    // Trivial pretend validation
    private static boolean validateInputFile(String fileName) throws IOException
    {
        System.out.println(" *** Pretending to validate the input file.");
        System.out.print(" *** Press Enter to continue and pass, R to fail validation, E to throw an exception ");
        int inInt = 0;
        try
        {
            inInt = System.in.read();
        } catch (IOException e)
        {
            e.printStackTrace();
            System.exit(1);
        }
        if (inInt == 'R' || inInt == 'r')
        {
            System.out.println(" *** Pretending that the file was rejected during validation.");
            return false;
        } else if (inInt == 'E' || inInt == 'e')
        {
            System.out.println(" *** Pretending that the file load threw an exception.");
            throw new IOException("User-simulated exception during file load.");
        }
        return true;
    }

    // trivial pretend processing
    private static int loadInputFile(String fileName) throws IOException
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
            // Don't catch exceptions; they should return upwards.
        }

        System.out.println(" +++ Pretending to load input file.  The file has " + counter + " lines.");

        System.out.print(" +++ Press Enter to continue and simulate a good load, E to throw an exception ");
        int inInt = 0;
        try
        {
            inInt = System.in.read();
        } catch (IOException e)
        {
            e.printStackTrace();
            System.exit(1);
        }
        if (inInt == 'e' || inInt == 'E')
        {
            System.out.println(" +++ Pretending that the file failed to load.");
            throw new IOException("User-simulated failure of file load.");
        }
        return counter;
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

