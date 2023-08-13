package cuit.ljzhang.jydb.backend.vm;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static cuit.ljzhang.jydb.common.Error.DeadlockException;

/**
 * @ClassName LockTable
 * @Description
 * @Author ljzhang
 * @Date 2023/8/4 21:12
 * @Version 1.0
 * 维护的依赖等待图
 *      ---进行死锁检测
 */
public class LockTable  {

    /*某个XID已经获得的资源的UID列表*/
    private Map<Long, List<Long>> x2u;

    /* UID被某个XID持有 */
    private Map<Long, Long> u2x;

    /* 正在等待 UID 的 XID 列表 */
    private Map<Long, List<Long>> wait;

    /* 正在等待资源的XID的锁 */
    private Map<Long, Lock> waitLock;

    /* XID正在等待的UID列表 */
    private Map<Long, List<Long>> waitU;

    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    private Map<Long, Integer> xidStamp;

    /* 时间戳 -- 用于标记 */
    private int stamp;

    /*
    * 不需要等待则返回null，否则返回锁对象
    * 会造成死锁则抛出异常
    * 不需要等待的情况
    *       --- uid资源已经被xid获取到了
    *       --- uid资源还没用被某个事务获取
    * 出现死锁抛出异常
    *       --- 需要等待，返回锁 lock.lock()阻塞线程
    * */
    public Lock add(long xid, long uid) throws Exception{
       lock.lock();
        try {
            if(isInList(x2u, xid, uid)){
                return null;
            }
            if(!u2x.containsKey(uid)){
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            /*等待需要做的操作*/
            putIntoList(waitU, xid, uid);
            putIntoList(wait, uid, xid);
            if(hasDeadLock()){
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw DeadlockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }

    /* 死锁检测 */
    public boolean hasDeadLock(){
        xidStamp = new HashMap<>();
        stamp = 1;
        for(Long xid : x2u.keySet()){
            Integer xid_stamp = xidStamp.get(xid);
            /*已经搜索过的节点*/
            if(xid_stamp != null && xid_stamp > 0){
                continue;
            }
            stamp++;
            if(dfs(xid)){
                return true;
            }
        }
        return false;
    }

    /*
    * 深搜
    * T ---> U 一对多
    * U ---> T 一对一
    * 可以跑拓扑序列
    *       --- 但是需要额外维护入度
    * */
    public boolean dfs(long xid){
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp)
            return true;
        if(stp != null && stp < stamp){
            return false;
        }
        xidStamp.put(xid, stamp);
        List<Long> uids = waitU.get(xid);
        if(uids == null)
            return false;
        for(Long uid : uids){
            Long next_id = u2x.get(uid);
            assert next_id != null;
            if(dfs(next_id))
                return true;
        }
        return false;
    }

    /*更新list的几个操作*/

    private void removeFromList(Map<Long, List<Long>> listMap, long id0, long id1) {
        List<Long> l = listMap.get(id0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == id1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(id0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long id0, long id1) {
        if(!listMap.containsKey(id0)) {
            listMap.put(id0, new ArrayList<>());
        }
        listMap.get(id0).add(0, id1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long id0, long id1) {
        List<Long> l = listMap.get(id0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == id1) {
                return true;
            }
        }
        return false;
    }

    /*事务提交之后， 需要释放他的uid资源*/
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                /*需要释放xid占用的uid资源*/
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            /*释放 xid 等待的uid资源 abort抛弃事务时可能还有等待的uid资源 */
            waitU.remove(xid);
            /*释放 xid 持有的uid资源 abort抛弃事务时可能会持有锁 */
            x2u.remove(xid);
            /*
            *释放锁资源
            *TODO:有问题啊 --- 没用lock.lock()之后没用lock.unlock()
            * */
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    /*从等待中的xid队列中选择一个uid来占有uid*/
    public void selectNewXID(long uid){
        /*移除uid当前占有的xid*/
        u2x.remove(uid);
        List<Long> list = wait.get(uid);
        if(list == null) return;
        assert list.size() > 0;
        while(list.size() > 0){
            long xid = list.remove(0);
            if(!waitLock.containsKey(xid)){
                continue;
            }
            u2x.put(uid, xid);
            Lock lock = waitLock.remove(xid);
            /*xid等待获取的uid资源列表可以去掉uid了*/
            removeFromList(waitU, xid, uid);
            /*xid已经拿到uid资源了，直接解锁*/
            lock.unlock();
            break;
        }
        if(list.size() == 0)
            wait.remove(uid);
    }


}
