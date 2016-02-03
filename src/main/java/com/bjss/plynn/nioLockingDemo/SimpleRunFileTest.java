package com.bjss.plynn.nioLockingDemo;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class SimpleRunFileTest {


    public static void main(String[] args)
    {
        Logger myLog = LoggerFactory.getLogger(SimpleRunFileTest.class);
        if (args.length < 1) {
            myLog.error("Must enter a file name; there are no arguments.");
            System.exit(1);
        }
        Path fp = FileSystems.getDefault().getPath(args[0]);
        if (!Files.isRegularFile(fp) || !Files.isReadable(fp)) {
            myLog.error("PID {}, file {}: File {} does not exist or is not readable", new String[]
                    {ManagementFactory.getRuntimeMXBean().getName(), fp.toString(), fp.toString()});
            System.exit(1);
        }


        String choice = null;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Character ch;
        FileChannel fc = null;
        FileLock fl = null;

        Path fpLock = FileSystems.getDefault().getPath(args[0]);
        if (fpLock == null) {
            myLog.error("PID {}, file {}: Unable to find input file '{}'", new String[]
            {ManagementFactory.getRuntimeMXBean().getName(), fp.toString(), args[0]});
            System.exit(1);
        }


        try {
            fc = new RandomAccessFile(fp.toString() + ".lock", "rw").getChannel();
            myLog.info("Got the FileChannel: " + fc);
        } catch (Exception e) {
            myLog.error("PID {}, file {}: Can't open the FileChannel.",
                    ManagementFactory.getRuntimeMXBean().getName(), fp.toString());
            myLog.error(e.toString());
            System.exit(1);
        }

        do {
            System.out.print("Enter L to lock, W to wait for a lock, or U to unlock: ");
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
                        fl.release();
                    } catch (IOException e) {
                        myLog.error("PID {}, file {}: Failed releasing the lock/",
                            ManagementFactory.getRuntimeMXBean().getName(), fp.toString());
                        myLog.error(e.toString());
                        System.exit(1);
                        break;
                    }
                    fl = null;
                    System.out.println(" Lock was released.");
                    break;

                case 'L':
                case 'W':
                    System.out.println(" Locking...\n");
                    try {
                        if (ch.equals('L')) {
                            fl = fc.tryLock();
                        } else {
                            fl = fc.lock();
                        }
                    } catch (Exception e) {
                        myLog.error("PID {}, file {}: Exception trying to lock the file.",
                                ManagementFactory.getRuntimeMXBean().getName(), fp.toString());
                        myLog.error(e.toString());
                        System.exit(1);
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
            myLog.error("PID {}, file {}: For some reason there was an exception closing the channel.",
                ManagementFactory.getRuntimeMXBean().getName(), fp.toString());
            myLog.error(e.toString());
        }
    }
}


