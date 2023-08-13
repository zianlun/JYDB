package cuit.ljzhang.jydb.backend.vm;

import cuit.ljzhang.jydb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName Transaction
 * @Description
 * @Author ljzhang
 * @Date 2023/8/4 21:03
 * @Version 1.0
 * version manager 对事务的抽象
 */
public class Transaction {
    /*事务编号*/
    public long xid;
    /*
    * 对下列属性的猜测：
    *       level: 事务的隔离级别
    *               --- 0 读已提交
    *               --- 1 可重复读
    *       snapshot:在当前事务开始时仍然活跃的事务id
    *       autoAborted：是否自动撤销回滚事务
    * */
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    /*
    * 0号事务的等级为0 也就是超级事务
    * */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }

}
