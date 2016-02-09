package com.bjss.plynn.nioLockingDemo;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;

public class ConnectionFreeTest extends AbstractLoggedTest
{
    private String testFileName = "FileForJunitTesting.txt";
    private ExecutionPersister ep;
    private Connection conn;

    @BeforeMethod
    public void setUp() throws IOException, ClassNotFoundException
    {
        ep = new ExecutionPersister(testFileName);
    }
    @AfterMethod
    public void tearDown() throws Exception
    {
        ep.close();
        ep = null;
    }

    @Test
    public void connectTest() throws IOException, ClassNotFoundException, SQLException
    {
        Connection conn = ep.ConnectToDB();
    }

    @Test (expectedExceptions = java.lang.IllegalStateException.class, sequential = true)
    public void failWithoutClaimShouldThrowException() throws SQLException
    {
        ep.failFile();
    }

    @Test (expectedExceptions = java.lang.IllegalStateException.class, sequential = true)
    public void completeWithoutClaimShouldThrowException() throws SQLException
    {
        ep.completeFile();
    }

}
