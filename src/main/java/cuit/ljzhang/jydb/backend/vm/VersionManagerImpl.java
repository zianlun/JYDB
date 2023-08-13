package cuit.ljzhang.jydb.backend.vm;

import cuit.ljzhang.jydb.backend.common.AbstractCache;
import cuit.ljzhang.jydb.backend.dm.DataManager;
import cuit.ljzhang.jydb.backend.tm.TransactionManager;
import cuit.ljzhang.jydb.backend.tm.TransactionManagerImpl;
import cuit.ljzhang.jydb.backend.utils.Panic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static cuit.ljzhang.jydb.common.Error.ConcurrentUpdateException;
import static cuit.ljzhang.jydb.common.Error.NullEntryException;

/**
 * @ClassName VersionManagerImpl
 * @Description
 * @Author ljzhang
 * @Date 2023/8/4 20:59
 * @Version 1.0
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    /*封装有查询事务状态的方法*/
    TransactionManager tm;
    /*底层的数据管理*/
    DataManager dm;
    /*活跃的事务*/
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    /**/
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        /*Todo:为什么要把超级事务放进来*/
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    /*
    * 从缓存中获取一个entry
    *   --- getFoCache ： 会调用loadEntry
    *   --- 然后从dataitem中根据uid到缓存和文件中查
    *   --- 如果被逻辑删除会返回空
    * */

    @Override
    protected Entry getFoCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) {
            throw NullEntryException;
        }
        return entry;
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    /*强行移除缓存*/
    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    /*
    * 根据记录uid读取记录
    *     事务xid判断可见性
    * */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();
        if(transaction.err != null){
            throw transaction.err;
        }
        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid);
            } catch(Exception e) {
                t.err = ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            /*已经被删除*/
            if(entry.getXmax() == xid) {
                return false;
            }

            /*是否发生了版本跳跃*/
            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(
                    xid,
                    level,
                    activeTransaction
            );
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }
        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);

    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }


}
