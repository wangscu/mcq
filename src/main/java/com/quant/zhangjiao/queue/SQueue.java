package com.quant.zhangjiao.queue;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SQueue {
    private Logger logger = Logger.getLogger(SQueue.class);
    private String name;
    private AtomicLong readIdx = new AtomicLong(0);
    private AtomicLong writeIdx = new AtomicLong(0);
    private Map<Integer, DataStore> storeMap = Maps.newHashMap();
    private Thread thread;
    private final Lock writeLock = new ReentrantLock();
    private final Condition empty = writeLock.newCondition();
    private List<String> writeMsgList = Lists.newArrayList();
    private RandomAccessFile metaRaf;

    public SQueue(String name) throws Exception {
        this.name = "/opt/data/zhangjiao/" + name;
        initMeta();
        initAppender();
    }

    public void set(String body) throws Exception {
        try {
            writeLock.lock();
            writeMsgList.add(body);
            empty.signalAll();
        } finally {
            writeLock.unlock();
        }
    }

    public String get() throws Exception {
        long rIdx = readIdx.get();
        long wIdx = writeIdx.get();
        if (wIdx - rIdx <= 1) {
            return null;
        }

        if (!this.readIdx.compareAndSet(rIdx, rIdx + 1)) {
            return null;
        }

        DataStore readStore = getReadStore();
        int msgIdx = (int) (readIdx.get() % DataStore.MSG_SIZE_PER_FILE);
        String msg = readStore.get(msgIdx);
        if (Strings.isNullOrEmpty(msg)) {
            return null;
        }
        return msg;
    }

    private void initMeta() throws IOException {
        File metaFile = new File(name + ".meta");
        if (!metaFile.exists()) {
            metaFile.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(new File(name + ".meta"), "rwd");
            raf.setLength(1024);
            raf.seek(0);
            raf.writeLong(0);
            raf.writeLong(0);
            raf.getFD().sync();
            raf.close();
        }
        this.metaRaf = new RandomAccessFile(new File(name + ".meta"), "rwd");
        long rIdx = metaRaf.readLong();
        this.readIdx = new AtomicLong(rIdx);

        long wIdx = metaRaf.readLong();
        this.writeIdx = new AtomicLong(wIdx);
        logger.info("initMeta:" + this.toString());
    }

    private DataStore getReadStore() throws IOException {
        int storeIdx = (int) (this.readIdx.get() / DataStore.MSG_SIZE_PER_FILE);

        int preStoreIdx = storeIdx - 1;
        if (storeMap.containsKey(preStoreIdx)) {
            DataStore preDataStore = storeMap.remove(preStoreIdx);
            preDataStore.delete();
            saveMeta();
        }

        return getStore(storeIdx);
    }

    private DataStore getStore(int storeIdx) throws IOException {
        if (storeMap.containsKey(storeIdx)) {
            return storeMap.get(storeIdx);
        }

        DataStore dataStore = new DataStore(this.name, storeIdx);
        storeMap.put(storeIdx, dataStore);
        return dataStore;
    }

    private void saveMeta() throws IOException {
        this.metaRaf.seek(0);
        this.metaRaf.writeLong(readIdx.get());
        this.metaRaf.writeLong(writeIdx.get());
    }

    private void initAppender() {
        thread = new Thread(() -> {
            while (true) {
                List<String> oldList = null;
                try {
                    writeLock.lock();
                    oldList = this.writeMsgList;
                    this.writeMsgList = Lists.newArrayList();
                    if (null == oldList || oldList.isEmpty()) {
                        this.empty.await();
                    }
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    writeLock.unlock();
                }

                try {
                    if (null != oldList && !oldList.isEmpty()) {
                        appendMsg(oldList);
                    }
                    saveMeta();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
        thread.setName("SQueue-appender-" + this.name);
        thread.start();
    }

    private void appendMsg(List<String> writeMsgList) throws Exception {
        long idx = writeIdx.get();
        List<String> storeMsgList = Lists.newArrayList();
        for (String msg : writeMsgList) {
            storeMsgList.add(msg);
            int msgIdx = (int) (idx % DataStore.MSG_SIZE_PER_FILE);
            if (msgIdx >= DataStore.MSG_SIZE_PER_FILE - 1) {
                int storeIdx = (int) (writeIdx.get() / DataStore.MSG_SIZE_PER_FILE);
                DataStore dataStore = getStore(storeIdx);
                dataStore.append((int) (writeIdx.get() % DataStore.MSG_SIZE_PER_FILE), storeMsgList);
                writeIdx.addAndGet(storeMsgList.size());
                storeMsgList = Lists.newArrayList();
            }
            idx++;
        }

        if (storeMsgList.size() <= 0) {
            return;
        }
        int storeIdx = (int) (writeIdx.get() / DataStore.MSG_SIZE_PER_FILE);
        DataStore dataStore = getStore(storeIdx);
        dataStore.append((int) (writeIdx.get() % DataStore.MSG_SIZE_PER_FILE), storeMsgList);
        writeIdx.addAndGet(storeMsgList.size());
    }


    @Override
    public String toString() {
        return this.name + "|" + this.readIdx + "|" + this.writeIdx;
    }

}
