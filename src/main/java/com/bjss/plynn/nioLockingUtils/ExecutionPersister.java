package com.bjss.plynn.nioLockingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.Properties;

public class ExecutionPersister implements AutoCloseable
{
    // DB configuration constants
    static final String DBPROPERTIESFILE = "db.properties";
    static final String COLNAME_FILENAME = "name";
    static final String VARNAME_CLAIMED = "got_it";
    static final String VARNAME_PREVSTATUS = "prev_status";
    static final String VARNAME_MISSING = "file_missing";
    static final String VARNAME_TRIES = "prev_tries";

    // DB parameters read from file
    String myFileName;
    String JDBC_USER;
    String JDBC_PASS;
    String JDBC_DRIVER;
    String JDBC_DB_URL;

    Logger myLog;
    private int previousTries = -1;
    private Boolean haveFileClaimed;

    public ExecutionPersister(String name) throws IOException, ClassNotFoundException
    {
        myFileName = name;
        myLog = LoggerFactory.getLogger(ExecutionPersister.class);

        Properties prop = new Properties();
        InputStream iStream = ExecutionPersister.class.getClassLoader().getResourceAsStream(DBPROPERTIESFILE);
        if (iStream == null)
        {
            throw new FileNotFoundException("Property file " + DBPROPERTIESFILE + "was not found in the classpath");
        }
        prop.load(iStream);
        iStream.close();

        //  Database credentials"
        JDBC_USER = prop.getProperty("db.jdbc.user", "NULL");
        JDBC_PASS = prop.getProperty("db.jdbc.password", "NULL");
        JDBC_DRIVER = prop.getProperty("db.jdbc.driver", "NULL");
        JDBC_DB_URL = prop.getProperty("db.jdbc.url", "NULL");
        //Register JDBC driver
        Class.forName(JDBC_DRIVER);
        haveFileClaimed = false;
    }

    /*
     *  Code for exercising the Execution Persister
     */
    static public void main(String args[])
    {
        ExecutionPersister myEP = null;
        String fileName = "testing.txt";

        try
        {
            myEP = new ExecutionPersister(fileName);
            myEP.claimFile();

            myEP.failFile();

            myEP.claimFile();
            myEP.claimFile();

            myEP.completeFile();

            System.out.println("Post-completed call to claimFile should fail.  The result is "
                    + myEP.claimFile());
        } catch (Exception e) {
            // Need a PrintStream to get the stack trace into the logger.
            Logger myLog = LoggerFactory.getLogger(ExecutionPersister.class);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            e.printStackTrace(ps);

            if (myEP != null)
            {
                myLog.error("Exception caught in the file locking code.  File "  + fileName + " has " + myEP.getPreviousTries()
                        + " previous failed runs.  Cause follows:");
            } else {
                myLog.error("Exception caught in the file locking code. Unable to access count of previous runs.  Cause follows:");
            }
            myLog.error(baos.toString());
        }

        System.out.println("");
    }

    protected Connection ConnectToDB() throws SQLException
    {
        Connection conn;
        //Open a connection
        myLog.debug("Connecting to database");
        conn = DriverManager.getConnection(JDBC_DB_URL, JDBC_USER, JDBC_PASS);
        return conn;
    }

    public Boolean claimFile() throws SQLException
    {
        myLog.debug("Accessing database to claim file");
        Boolean claimSucceeded;
        if (haveFileClaimed)
        {
            throw new IllegalStateException("Attempt to claim file " + myFileName + " but it is already claimed by this process");
        }

        try (Connection conn = ConnectToDB();
             CallableStatement doPC = conn.prepareCall("{call claim_file (?, ?, ?, ?)}"))
        {
            doPC.setString(COLNAME_FILENAME, myFileName);
            doPC.registerOutParameter(VARNAME_CLAIMED, Types.BOOLEAN);
            doPC.registerOutParameter(VARNAME_PREVSTATUS, Types.VARCHAR);
            doPC.registerOutParameter(VARNAME_TRIES, Types.INTEGER);
            doPC.executeUpdate();
            claimSucceeded = doPC.getBoolean(VARNAME_CLAIMED);
            previousTries = doPC.getInt(VARNAME_TRIES);
            if (myLog.isDebugEnabled())
            {
                String prevStatus = doPC.getString(VARNAME_PREVSTATUS);
                if (prevStatus == null)
                {
                    prevStatus = "null";
                }
                myLog.debug("File " + myFileName + " was previously " + prevStatus
                        + " for " + previousTries
                        + " tries. The return from the stored procedure execution was " + claimSucceeded);
            }
        }   // AutoCloseable resources closed on leaving the try block

        haveFileClaimed = claimSucceeded;
        return claimSucceeded;
    }

    public void failFile() throws SQLException
    {
        myLog.debug("Accessing database to mark file as failed");
        if (! haveFileClaimed)
        {
            throw new IllegalStateException("Attempt to fail file " + myFileName + " but it was not claimed by this process");
        }

        try (Connection conn = ConnectToDB();
             CallableStatement doPC = conn.prepareCall("{call fail_file (?, ?)}"))
        {
            doPC.setString(COLNAME_FILENAME, myFileName);
            doPC.registerOutParameter(VARNAME_MISSING, Types.BOOLEAN);
            doPC.executeUpdate();

            if (doPC.getBoolean(VARNAME_MISSING)) {
                throw new IllegalStateException("Attempt to fail file " + myFileName +" but it was not found in the database");
            }
            myLog.debug("File " + myFileName + " was marked failed.");
        }
        haveFileClaimed = false;
    }

    public void completeFile() throws SQLException
    {
        myLog.debug("Accessing database to mark file as completed");
        if (! haveFileClaimed)
        {
            throw new IllegalStateException("Attempt to complete file " + myFileName + " but it was not claimed by this process");
        }

        try (Connection conn = ConnectToDB();
             CallableStatement doPC = conn.prepareCall("{call complete_file (?, ?)}"))
        {
            doPC.setString(COLNAME_FILENAME, myFileName);
            doPC.registerOutParameter(VARNAME_MISSING, Types.BOOLEAN);
            doPC.executeUpdate();

            if (doPC.getBoolean(VARNAME_MISSING))
            {
                throw new IllegalStateException("Attempt to complete file " + myFileName + " but it was not found in the database");
            }
            myLog.debug("File " + myFileName + " was marked completed.");
        }
        haveFileClaimed = false;
    }

    public int getPreviousTries ()
    {
        return previousTries;
    }

    @Override
    public void close() throws Exception
    {
        // Nothing really to release yet.  Added for AutoCloseable
        ;
    }
}