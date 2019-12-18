package com.quant.zhangjiao.queue;

import org.junit.jupiter.api.*;

public class SQueueTest {
    @BeforeAll
    static void initAll() {
    }

    @BeforeEach
    void init() {
    }

    @Test
    void succeedingTest() throws Exception {
    }

    @Test
    void succeedingTestRoll() throws Exception {
        SQueue sQueue = new SQueue("test");
        sQueue.set("hello1");
        System.out.println(sQueue.get());
        sQueue.set("hello2");
        System.out.println(sQueue.get());
        sQueue.set("hello3");
        System.out.println(sQueue.get());
        sQueue.set("hello4");
        System.out.println(sQueue.get());
        sQueue.set("hello5");
        System.out.println(sQueue.get());
        sQueue.set("hello6");
        System.out.println(sQueue.get());
        sQueue.set("hello7");
        System.out.println(sQueue.get());
    }

    @AfterEach
    void tearDown() {
    }

    @AfterAll
    static void tearDownAll() {
    }

}
