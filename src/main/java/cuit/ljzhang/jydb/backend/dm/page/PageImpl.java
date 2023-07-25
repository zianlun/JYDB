package cuit.ljzhang.jydb.backend.dm.page;

import cuit.ljzhang.jydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @ClassName PageImpl
 * @Description
 * @Author ljzhang
 * @Date 2023/7/25 20:09
 * @Version 1.0
 * 页面缓存结构
 *      默认数据页大小： 8k = 1024 * 2^3
 */
public class PageImpl implements Page{

    //该页面的页号
    private int pageNumber;

    //页实际包含的字节数据
    private byte[] data;

    //是否为脏页面  --- 脏页面：表示被修改过的页面，做缓存驱逐的时候需要将脏页面写回磁盘
    private boolean dirty;

    //锁  --- 实现类选择ReentrantReadWritelock
    private Lock lock;

    /*
    * 拿到Page的引用之后可以快速对这个页面的缓存进行释放操作
    * 这个有点小六的设计模式：
    *       PageCache构造了Page之后将自己的引用交给PageCache
    * */
    private PageCache pc;

    public PageImpl(){}

    public PageImpl(int pageNumber, byte[] data){
        this.pageNumber = pageNumber;
        this.data = data;
        lock = new ReentrantLock();
    }

    public PageImpl(int pageNumber, byte[] data, PageCache pc){
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    @Override
    public void setDirty(boolean isDirty) {
        this.dirty = false;
    }

    @Override
    public boolean isDirty(){
        return this.dirty;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void release() {
        pc.release(this);
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }

}
