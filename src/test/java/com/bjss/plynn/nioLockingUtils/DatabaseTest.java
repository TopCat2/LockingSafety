package com.bjss.plynn.nioLockingUtils;

import com.bjss.plynn.AbstractLoggedTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseTest extends AbstractLoggedTest
{
    private String testFileName = "FileForConnectionTesting.txt";
    private ExecutionPersister ep;

    /*
      *  These tests depend on starting with a specific record not in the database.
      *  The method setup updates the database by deleting the test record.
      *  Questionable practice, but removes the need for a database stub.
      *  Tests must be sequential because they access the same database record.
     */

    private void removeRecord() throws SQLException
    {
        Connection conn = ep.ConnectToDB();
        CallableStatement doPC = conn.prepareCall("DELETE FROM files_processed WHERE file_name = ?");
        doPC.setString(1, testFileName);
        doPC.executeUpdate();
        doPC.close();
    }
    @BeforeMethod
    public void setUp() throws IOException, ClassNotFoundException, SQLException
    {
        ep = new ExecutionPersister(testFileName);
        removeRecord();
        }

    @AfterMethod
    public void tearDown() throws Exception
    {
        ep.close();
        ep = null;
    }

   @Test (groups = "useDB", expectedExceptions = java.lang.IllegalStateException.class, sequential = true)
    public void failMissingFileShouldThrowException() throws Exception
    {
        assert(ep.claimFile());
        removeRecord();         // Delete the record; it should not be found by the next step.
        ep.failFile();
    }

   @Test (groups = "useDB", expectedExceptions = java.lang.IllegalStateException.class, sequential = true)
    public void completeMissingFileShouldThrowException() throws Exception
    {
        assert(ep.claimFile());
        removeRecord();         // Delete the record; it should not be found by the next step.
        ep.completeFile(7);
    }

   @Test (groups = "useDB", sequential = true)
    public void validSequence() throws Exception
    {
        // Claim and fail the file; previous should be zero.
        assert(ep.claimFile());
        assert(ep.getPreviousTries() == 0);
        ep.failFile();

        // Claim and fail the file; previous should be one.
        assert(ep.claimFile());
        assert(ep.getPreviousTries() == 1);
        ep.failFile();

        // Claim and complete the file; previous should be two.
        assert(ep.claimFile());
        assert(ep.getPreviousTries() == 2);
        ep.completeFile(7);
    }

   @Test (groups = "useDB", sequential = true)
    public void claimCompleteShouldBeFalse() throws Exception
    {
        assert(ep.claimFile());
        assert(ep.getPreviousTries() == 0);
        ep.completeFile(7);

        // Claiming a completed file should return false
        assert(! ep.claimFile());
    }

}
