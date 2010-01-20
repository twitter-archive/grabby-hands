/** Copyright 2010 Twitter, Inc. */
package com.twitter.grabbyhands;

public class JavaTest {
    public JavaTest() {
        System.out.println("java test");
    }

    public void run() {
        testCreate();
        testWriteRead();
    }

    protected void testCreate() {
        System.out.println("run testCreate");
        String[] servers = { "localhost:22133" };
        String[] queues = { "grabby_test" };
        GrabbyHands grabbyHands = new GrabbyHands(
            servers, queues, 16384, 1, 1, 1000, 1000);
        System.out.println("pass testCreate");
    }

    protected void testWriteRead() {
        System.out.println("run testWriteRead");
        // TODO: Implement
        System.exit(-1);
    }

    public static void main(String[] args) {
        JavaTest javaTest = new JavaTest();
        javaTest.run();
    }
}
