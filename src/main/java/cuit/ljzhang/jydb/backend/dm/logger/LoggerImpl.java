package cuit.ljzhang.jydb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import cuit.ljzhang.jydb.backend.utils.Panic;
import cuit.ljzhang.jydb.backend.utils.Parser;

import javax.swing.text.BadLocationException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static cuit.ljzhang.jydb.common.Error.BadLogFileException;

/**
 * @ClassName LoggerImpl
 * @Description
 * @Author ljzhang
 * @Date 2023/7/27 14:56
 * @Version 1.0
 * 日志二进制文件 ：
 *      [xCheckSum][Log1][Log2]...[LogN][BadTail]
 *      xCheckSum:对后续所有日志计算的校验和
 *      Log:日志记录
 *      Badtail:数据库崩溃时，未来得及写入的日志数据，不一定存在
 * 日志格式如下：
*       [Size][Checksum][Data]
*          ---size:四字节整数，标识Data字段字节数
*          ---Checksum: 改条日志的校验和
*          ---Data：数据字段
 */
public class LoggerImpl implements Logger{
    /*校验种子*/
    private static final int SEED = 13331;

    private static final int OF_LOG = 4;

    /*size，checksum，data的偏移字节位置*/
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;

    /*当前日志文件的记录位置*/
    private long position;
    /*初始化时记录， logg日志插入不更新*/
    private long fileSize;
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.raf = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /*获取日志文件大小 初始化参数和校验和  校验和的检验*/
    void init() {
        long size = 0;
        try {
            size = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;
        checkAndRemoveTail();
    }

    /*校验和*/
    private int calChecksum(int xCheck, byte[] log){
        for(byte b : log){
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /*打开日志文件时：校验日志文件的校验和*/
    private void checkAndRemoveTail(){
        rewind();
        int xCheck = 0;
        while(true){
            byte[] log = internNext();
            if(log == null)break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum){
            Panic.panic(BadLogFileException);
        }
        /*截断到正常日志末尾*/
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            raf.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    /*不断从文件中读取下一条日志，并将其中的 Data 解析出来并返回 */
    private byte[] internNext(){
        /*当前指针的位置 + 接下来一条日志的数据偏移 >= 文件的大小 ： 没有日志记录了，返回空*/
        if(position + OF_DATA >= fileSize){
            return null;
        }
        /*读取日志的size*/
        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(buf.array());
        /*检验一下解析出的size字段描述的data字段是否超过了文件的大小*/
        if(position + OF_DATA + size >= fileSize){
            return null;
        }
        /*读取记录*/
        ByteBuffer data = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(data);
        } catch (IOException e) {
            Panic.panic(e);
        }
        /*对读取的日志进行校验*/
        byte[] log = data.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if(checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    /*
    * 将日志写入日志文件
    * 包裹日志数据，写入文件之后，更新校验和
    * 更新校验和后，刷新缓冲区，保证内容吸入缓冲区
    * */
    @Override
    public void log(byte[] data){
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    /*包装日志数据*/
    private byte[] wrapLog(byte[] data) {
        /*计算大小和检验和字段然后拼接*/
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    /*更新校验和*/
    private void updateXChecksum(byte[] log) {
        /*计算加入当前日志之后的一个校验和*/
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            /*写入校验和*/
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            /*刷新缓冲区*/
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            /*返回的时日志记录的data字段*/
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
           /*
           * FileChannel的truncate()方法是Java NIO（New I/O）库中的一个方法，
           * 用于截断（缩减）文件的大小。
           * 该方法允许将一个已打开的文件截断到一个指定的大小。
           * */
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

}
