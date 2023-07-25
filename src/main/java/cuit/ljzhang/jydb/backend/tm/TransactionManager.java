package cuit.ljzhang.jydb.backend.tm;

import cuit.ljzhang.jydb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static cuit.ljzhang.jydb.backend.tm.TransactionManagerImpl.XID_HEADER_POSITION;
import static cuit.ljzhang.jydb.common.Error.*;

/**
 * @ClassName TransactionManager
 * @Description
 * @Author ljzhang
 * @Date 2023/7/23 12:14
 * @Version 1.0
 *
 * TM:通过维护XID文件来维护事务的状态
 *    并且提供接口供给其他模块来查询某个事务的状态
 * XID:
 *    保存了受到管理的事务的数量
 *    以及对应位置的事务的状态
 * 文件操作：
 *    随机文件访问处理流
 *    RandomAccessFile 和 FileChannel
 *
 */
public interface TransactionManager {

    /*开启一个新事务*/
    long begin();
    /*提交事务*/
    void commit(long xid);
    /*取消事务*/
    void abort(long xid);
    /*查询事务的状态是否为是正在进行*/
    boolean isActive(long xid);
    /*查询事务的状态是否是已提交*/
    boolean isCommitted(long xid);
    /*查询书屋状态是否是已取消*/
    boolean isAborted(long xid);
    /*关闭TM*/
    void close();

    /*
    * 创建一个xid文件并创建TM
    * */
    public static TransactionManagerImpl create(String path){
        File f_xid = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            /*
            *如果指定的文件路径不存在，且文件名合法，那么将会创建一个新的空文件。
            *如果指定的文件路径已经存在同名的文件, 那么抛出文件存在异常
            * */
            if(!f_xid.createNewFile()){
                Panic.panic(FileExistsException);
            }
        } catch (IOException e) {
           Panic.panic(e);
        }
        /*
        *根据文件系统的权限和文件的状态判断文件
        *是否可读和可写
        * */
        if(!f_xid.canRead() || !f_xid.canWrite()){
            Panic.panic(FileCannotRWException);
        }
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(f_xid, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        /*
        * header的大小为8字节
        * */
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(XID_HEADER_POSITION);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }

    /*
    * 打开一个存在的xid文件
    * */
    public static TransactionManagerImpl open(String path){
        File f_xid = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if(!f_xid.exists()){
            Panic.panic(FileNotExistsException);
        }
        if(!f_xid.canRead() || !f_xid.canWrite()){
            Panic.panic(FileCannotRWException);
        }
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(f_xid, "rw");
            fc = raf.getChannel();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new TransactionManagerImpl(raf, fc);
    }
}
