package com.bjss.plynn.nioLockingDemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBPersister {
    Logger myLog = null;

    Boolean usable;         // Object can connect to a database
    Boolean fileAlreadyProcessed;   // File record was already completed and cannot be run with this name
    Boolean inFlight;       // File record can be used
    int prevAttempts;       // Number of previous failed attempts to run this file
    String savedFileName;   // Name of currently held file record

    // Since this gets a database connection, there should be a factory.
    // Especialy to allow the possibility of a pool.
    DBPersister() {
        usable=true;
        fileAlreadyProcessed = false;
        inFlight = false;
        prevAttempts = -1;
        savedFileName = null;
        myLog = LoggerFactory.getLogger(DBPersister.class);
    }

    Boolean beginProcessingFile(String fileName) {
        myLog.info("Database call to start processing the file named {}", fileName);
        // The actual code should probably not hold a lock on the record - very long write locks are for server logs.
        // *** Decide if a "failed" file name can be run again, or if the file name must be changed.

        // Stub test code - even chance that it is in use
        if (Math.random() < 0.5)
        {
            fileAlreadyProcessed = true;
            inFlight = false;
            usable = false;
            return false;
        }
        // Stub test code - have there been previous, uncompleted runs of this file?
        prevAttempts = Math.min((int) (Math.random() * 6) - 3, 0);

        inFlight = true;
        savedFileName = fileName;
        myLog.info("Record for {} persisted to database and set as 'running'", fileName);
        return true;
    }

    Boolean markFileFinished() {
        if (! usable || !inFlight)
        {
            myLog.error("Invalid markFileFinished call to DBPersister with no current file");
            return false;
        }

        myLog.info("Database call to mark the file named {} as having finished processing", savedFileName);
        inFlight = false;
        myLog.info("Existing database record for {} set as 'completed'", savedFileName);
        return true;
    }

    Boolean markFileFailed() {
        if (! usable || !inFlight)
        {
            myLog.error("Invalid markFileFailed call to DBPersister with no current file");
            return false;
        }

        myLog.info("Database call to mark the file named {} as having failed processing", savedFileName);
        inFlight = false;
        myLog.info("Existing database record for {} set as 'failed'", savedFileName);
        return true;
    }

    public Boolean isUsable()
    {
        return usable;
    }

    public Boolean canProcessFile()
    {
        return inFlight;
    }

    public int getPrevAttempts()
    {
        return prevAttempts;
    }
}
