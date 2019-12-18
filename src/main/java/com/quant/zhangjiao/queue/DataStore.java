package com.quant.zhangjiao.queue;

import io.netty.util.CharsetUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class DataStore {
    private Logger logger = Logger.getLogger(DataStore.class);

    private final static int MSG_BODY_LEN = 1024;
    public final static int DATA_FILE_LEN = 64 * 1024 * 1024;
    public final static int MSG_SIZE_PER_FILE = DATA_FILE_LEN / MSG_BODY_LEN;
    private RandomAccessFile dataFile;
    private FileChannel fc;
    private String dataFileName;

    public DataStore(String name, int idx) throws IOException {
        this.dataFileName = name + ".data." + idx;
        File file = new File(dataFileName);
        if (!file.exists()) {
            file.createNewFile();
            file = new File(dataFileName);
        }

        RandomAccessFile raf = new RandomAccessFile(file, "rwd");
        raf.setLength(DATA_FILE_LEN);
        raf.getFD().sync();
        raf.close();

        this.dataFile = new RandomAccessFile(file, "rwd");
        this.fc = dataFile.getChannel();
    }

    public String get(int idx) throws Exception {
        int pos = idx * MSG_BODY_LEN;
        ByteBuffer bb = ByteBuffer.allocate(MSG_BODY_LEN);
        this.read(fc, bb, pos);
        bb.position(0);
        int dataLen = bb.getInt();
        byte[] data = new byte[dataLen];
        bb.get(data, 0, dataLen);
        String msg = new String(data, CharsetUtil.UTF_8);
        return msg;
    }

    public void append(int idx, List<String> msgList) throws Exception {
        logger.info(dataFileName + ":" + idx + ":" + msgList.size());
        int pos = idx * MSG_BODY_LEN;
        ByteBuffer bb = ByteBuffer.allocate(MSG_BODY_LEN * msgList.size());
        for (int i = 0; i < msgList.size(); i++) {
            String msg = msgList.get(0);
            bb.position(i * MSG_BODY_LEN);
            byte[] data = msg.getBytes(CharsetUtil.UTF_8);
            int dataLen = data.length;
            bb.putInt(dataLen);
            bb.put(data);
        }
        bb.position(0);
        this.write(fc, pos, bb);
    }

    public void delete() throws IOException {
        fc.close();
        dataFile.close();
        new File(dataFileName).delete();
    }

    private long read(FileChannel ch, ByteBuffer bb, final long start_position) throws IOException {
        long position = start_position;
        while (bb.hasRemaining()) {

            int read = ch.read(bb, position);
            if (read >= 0) {
                position += read;

                if (read == 0) {
                    Thread.yield();
                }
            } else {
                return 0;
            }
        }

        return position - start_position;
    }

    private void write(final FileChannel ch, final long offset, final ByteBuffer bf) throws IOException {
        int size = 0;
        while (bf.hasRemaining()) {
            final int l = ch.write(bf, offset + size);
            size += l;
            if (l < 0) {
                break;
            }
        }
    }
}
