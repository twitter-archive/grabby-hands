/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        Config config = new Config(servers, queues, 1, 1, 1, 16384, 1000, 1000, 50);
        GrabbyHands grabbyHands = new GrabbyHands(config);
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
