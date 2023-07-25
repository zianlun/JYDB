package cuit.ljzhang.jydb.backend.tm;

import cuit.ljzhang.jydb.backend.utils.Panic;
import cuit.ljzhang.jydb.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static cuit.ljzhang.jydb.common.Error.BadXIDFileException;

/**
 * @ClassName TransactionManagerImpl
 * @Description
 * @Author ljzhang
 * @Date 2023/7/24 19:35
 * @Version 1.0
 * 长度单位：字节
 */
public class TransactionManagerImpl implements TransactionManager{
    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;

    //文件投位置 --- 保存着受管理事务的数量
    public static final long XID_HEADER_POSITION = 0;

    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    /*
    * FIELD_TRAN_ACTIVE：正在进行
    * FIELD_TRAN_COMMITTED：已提交
    * FIELD_TRAN_ABORTED：已撤销（回滚
    * */
    private static final byte FIELD_TRAN_ACTIVE   = 0;

    private static final byte FIELD_TRAN_COMMITTED = 1;

    private static final byte FIELD_TRAN_ABORTED  = 2;

    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;

    //xid文件后缀
    static final String XID_SUFFIX = ".xid";

    /*随机读取文件对象*/
    private RandomAccessFile raf;

    /*NIO: 支持随机访问文件，配合ByteBuffer高效传输*/
    private FileChannel fc;

    /*文件头部的信息：记录XID文件管理的个数
    * 注意：0号超级事务的状态不需要记录
    * 需要管理的事务id是从1开始记录的
    * */
    private long xidCounter;

    //锁 --- 在构造函数处初始化
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter(){
        /*真实文件长度*/
        long fileLen = 0;
        try {
            fileLen = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH){
            Panic.panic(BadXIDFileException);
        }
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            /*移动到文件头 偏移量为0字节*/
            fc.position(0);
            /*将文件表头信息读取到buf*/
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        /*读取到了XID文件头部存的文件大小：可以计算出最新事务的id*/
        this.xidCounter = Parser.parseLong(buf.array());
        /*需要管理的事务id是从1开始记录的*/
        long end = getXidPosition(this.xidCounter + 1);
        /*比较计算地址长度 和 文件实际长度*/
        if(end != fileLen){
            Panic.panic(BadXIDFileException);
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }


    /*开启一个事务 并返回XID*/
    @Override
    public long begin() {
        /*
        * 为什么要加锁：
        *   操作共享资源xid，并将修改内容写入到文件中
        *   必须保证线程是安全的
        * */
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.lock();
        }
    }


    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        /*关闭文件读取流*/
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /*更新xid事务的状态*/
    private void updateXID(long xid, byte status){
        long offset = getXidPosition(xid);
        byte[] data = new byte[XID_FIELD_SIZE];
        data[0] = status;
        /*将字节数组保证为ByteBuffer配合FileChannel进行文件的写入*/
        ByteBuffer buf = ByteBuffer.wrap(data);
        try {
            /*将状态写入文件*/
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            /*
            * metaData:是否写入文件的元数据
            * 文件的元数据：与文件相关的附加信息，包含文件的属性和描述信息
            * */
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    //将XID+1,并更新XID Header
    private void incrXIDCounter(){
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(XID_HEADER_POSITION);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*获取xid事务是否处于status状态*/
    public Boolean checkXID(long xid, byte status){
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }
}
