package com.bjss.plynn.nioLockingDemo;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.io.RandomAccessFile;    //  The io package connection needed to get to nio.
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;


public class RunLockTest {

    public static void main(String[] args)
    {
        System.out.println("Hello, world.");
        Logger myLog = LoggerFactory.getLogger(RunLockTest.class);
        String choice = null;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Character ch;
        FileChannel fc = null;
        FileLock fl = null;

        try {
            fc = new RandomAccessFile("/home/peter/test.txt", "rw").getChannel();
            myLog.info("Got the FileChannel: " + fc);
        } catch (Exception e) {
            myLog.error("Can't open the FileChannel.");
            myLog.error(e.toString());
            System.exit(1);
        }

        do {
            System.out.print("Enter L to lock or U to unlock: ");
            try {
                choice = br.readLine();
            } catch (IOException ioe) {
                System.out.println("IO error trying to read your choice!");
                System.exit(1);
            }
            if (choice.length() < 1) {
                ch = ' ';
            } else {
                ch = Character.toUpperCase(choice.charAt(0));
            }

            switch (ch) {
                case 'U':
                    System.out.println(" Unlocking...\n");
                    if (fl == null) {
                        System.out.println(" What the heck?  You don't have the lock.");
                        break;
                    }
                    try {
                        fl.close();
                    } catch (IOException e) {
                        myLog.error("Failed closing the lock/");
                        myLog.error(e.toString());
                        break;
                    }
                    fl = null;
                    System.out.println(" Lock was closed.");
                    break;

                case 'L':
                    System.out.println(" Locking...\n");
                    try {
                        fl = fc.tryLock();
                    } catch (IOException e) {
                        myLog.error("Exception trying to lock the file.");
                        myLog.error(e.toString());
                    }
                    if (fl == null) {
                        System.out.println("The lock was unavailable");
                    } else {
                        System.out.println("You got the lock: " + fl);
                    }
                    break;
                case 'Q':
                    System.out.println(" It was nice working with you.\n");
                    break;
                default:
                    System.out.println(" What was that: " + ch + "\n");
            }

        } while (choice.length() < 1 || ! choice.substring(0, 1).equals("q"));
        try {
            fc.close();
        } catch (IOException e) {
            myLog.error("For some reason there was an exception closing the channel.");
            myLog.error(e.toString());
        }
    }
}
