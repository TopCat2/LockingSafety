package com.bjss.plynn.nioLockingUtils;

import com.bjss.plynn.AbstractLoggedTest;
import org.testng.annotations.Test;

public class FilelessNioTest extends AbstractLoggedTest
{
    private final String BADFILENAME = "alskfjawoieFileForLockTesting.txt";

    @Test (expectedExceptions = java.io.FileNotFoundException.class)
    public void lockingNonexistentFileShouldFail() throws Exception
    {
        InputFileClaimer ifc = new InputFileClaimer();
        ifc.getLockFile(BADFILENAME);
    }

    @Test (expectedExceptions = IllegalStateException.class)
    public void unlockingUninitializedFileShouldFail() throws Exception
    {
        InputFileClaimer ifc = new InputFileClaimer();
        ifc.releaseLockFile();
    }

}
