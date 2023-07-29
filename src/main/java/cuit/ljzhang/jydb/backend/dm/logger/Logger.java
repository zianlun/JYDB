package cuit.ljzhang.jydb.backend.dm.logger;

import cuit.ljzhang.jydb.backend.utils.Panic;
import cuit.ljzhang.jydb.backend.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static cuit.ljzhang.jydb.common.Error.*;

/**
 * @ClassName Logger
 * @Description
 * @Author ljzhang
 * @Date 2023/7/27 14:56
 * @Version 1.0
 * 日志文件操作接口
 */
public interface Logger {

    /*记录日志*/
    void log(byte[] data);

    /*读取日志，迭代器模式*/
    byte[] next();

    /*移动position到日志记录开头*/
    void rewind();

    /*截断到正常日志末尾*/
    void truncate(long x) throws Exception;

    /*关闭日志  主要是两个流的关闭*/
    void close();

    /* 创建日志文件  */
    static Logger create(String path) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        /* 初始化校验和 */
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new LoggerImpl(raf, fc, 0);
    }

    /*打开日志文件  校验日志和参数*/
    public static Logger open(String path) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        if(!f.exists()) {
            Panic.panic(FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        LoggerImpl lg = new LoggerImpl(raf, fc);
        /*获取日志文件大小 初始化参数和校验和  校验和的检验*/
        lg.init();
        return lg;
    }

}
