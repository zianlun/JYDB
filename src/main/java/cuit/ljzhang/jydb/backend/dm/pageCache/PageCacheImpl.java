package cuit.ljzhang.jydb.backend.dm.pageCache;

import cuit.ljzhang.jydb.backend.common.AbstractCache;
import cuit.ljzhang.jydb.backend.dm.page.Page;
import cuit.ljzhang.jydb.backend.dm.page.PageImpl;
import cuit.ljzhang.jydb.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @ClassName PageCacheImpl
 * @Description
 * @Author ljzhang
 * @Date 2023/7/25 20:12
 * @Version 1.0
 * 页面缓存的具体实现类
 * ---需要继承缓存框架
 * ---需要实现根据具体的数据源获取数据和释放数据接口
 * ---本系统的数据源为文件系统
 *
 *
 * !!!与原文不同!!!
 *      文件上锁：
 *          ---ReentrantReadWriteLock
 *          ---读取的时候应该可以使用共享锁
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    private static final int MEM_MIN_LIM = 10;

    /*数据源文件后缀*/
    public static final String DB_SUFFIX = ".db";

    /*随机读取文件处理流*/
    private RandomAccessFile raf;
    private FileChannel fc;

    /*文件可重入式读写锁*/
    private ReentrantReadWriteLock rwLock;

    /*记录当前数据库文件的页数  -- 总页数  不只是当前在缓存中的*/
    private AtomicInteger pageNumbers;

    //Todo:构造函数


    /*从文件数据源中获取数据  并存进缓存页*/
    @Override
    protected Page getFoCache(long key) throws Exception {
        /*获取缓存页在文件中的偏移位置*/
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);
        /*分配内存空间  读取页数据*/
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        /*读取共享资源 --- 上读锁*/
        rwLock.readLock().lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            /*释放读锁*/
            rwLock.readLock().unlock();
        }
        return new PageImpl(pgno, buf.array(), this);
    }

    /*释放缓存页 根据dirty字段进行判断是否回源*/
    @Override
    protected void releaseForCache(Page page) {
        if(page.isDirty()){
            /*回源*/
            flush(page);
            page.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pageNumber = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pageNumber, initData);
        flush(page);
        return pageNumber;
    }

    @Override
    public Page getPage(int pageNumber) throws Exception {
        return get(pageNumber);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        super.release(page.getPageNumber());
    }

    //Todo: 待完成
    @Override
    public void truncateByBgno(int maxPagno) {

    }

    @Override
    public int getPageNumbers() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    /*获取页在文件中的存储位置*/
    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE;
    }

    /*对指定的缓存脏页进行回源*/
    private void flush(Page page){
        int pageNumber = page.getPageNumber();
        long offset = pageOffset(pageNumber);
        /*独占锁 -- 保证线程安全的*/
        rwLock.writeLock().lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            try {
                fc.force(false);
            } catch (IOException e) {
                Panic.panic(e);
            }
            rwLock.writeLock().unlock();
        }
    }

}
