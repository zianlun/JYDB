package cuit.ljzhang.jydb.backend.dm.dataItem;

import cuit.ljzhang.jydb.backend.common.SubArray;
import cuit.ljzhang.jydb.backend.dm.DataManagerImpl;
import cuit.ljzhang.jydb.backend.dm.page.Page;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * @ClassName DataItemImpl
 * @Description
 * @Author ljzhang
 * @Date 2023/7/28 19:33
 * @Version 1.0
 *
 */
public class DataItemImpl implements DataItem {

    /*三个字段的偏移位置*/
    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;

    private byte[] oldRaw;

    private DataManagerImpl dm;

    private ReentrantReadWriteLock rwLock;

    private long uid;

    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page page, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        rwLock = new ReentrantReadWriteLock();
        this.dm = dm;
        this.uid = uid;
        this.page = page;
    }

    /*为1标识逻辑删除*/
    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte)0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    /*
    * before、unbefore、after
    *       修改前需要调用before
    *       撤销修改需要调用unBefore
    *       修改完成需要调用after
    * 主要为了保证前相数据，并且执行日志，保证原子性
    * */
    @Override
    public void before() {
        rwLock.writeLock().lock();
        page.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw, raw.start, oldRaw.length);
        rwLock.writeLock().unlock();
    }

    /*修改操作 记录日志*/
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        rwLock.writeLock().unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        rwLock.writeLock().lock();
    }

    @Override
    public void unlock() {
        rwLock.writeLock().unlock();
    }

    @Override
    public void rLock() {
        rwLock.readLock().lock();
    }

    @Override
    public void rUnLock() {
        rwLock.readLock().unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
