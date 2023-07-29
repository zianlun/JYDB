package cuit.ljzhang.jydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static cuit.ljzhang.jydb.common.Error.CacheFullException;

/**
 * @ClassName AbstractCache
 * @Description
 * @Author ljzhang
 * @Date 2023/7/24 23:04
 * @Version 1.0
 * 引用缓存框架
 */
public abstract class AbstractCache<T> {

    //实际缓存的数据
    private HashMap<Long, T> cache;
    //资源引用个数
    private HashMap<Long, Integer> references;
    //正在被获取的资源
    private HashMap<Long, Boolean> getting;

    //缓存的最大缓存资源数
    private int maxResource;

    //缓存中的元素个数
    private int count;

    //锁
    private Lock lock = null;

    /*构造函数  初始化缓存hash*/
    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /*
    * 尝试获取资源
    * 操作储存时 -- 需要保证线程安全的
    * 循环无限尝试获取资源
    * */
    protected T get(long key) throws Exception {
        while(true){
            lock.lock();
            /*如果getting中存在key --- 表示其他资源正在从文件中尝试获取数据*/
            if(getting.containsKey(key)){
                /*解锁 -- 并休眠线程10毫秒 然后循环重新尝试*/
                lock.unlock();
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            /*缓存中存在key --- 直接获取数据*/
            if(cache.containsKey(key)){
                T obj = cache.get(key);
                /*此处修改也是线程不安全的 --- 加锁的必要性*/
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
            /*
            *尝试获取资源 --- 缓存已满就抛出异常
            * InnoDB中的LRU则时会淘汰LRU的末端节点
            *   --- yang区 and old区
            *   --- 在old区存活超过一秒 被读取进入yang区
            *   --- yang区前1/4不会重新移动到头节点
            * */
            if(maxResource > 0 && count == maxResource){
                lock.unlock();
                throw CacheFullException;
            }
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }
        T obj = null;
        /*资源不在缓存中的获取情况*/
        try {
            obj = getFoCache(key);
        } catch (Exception e) {
            lock.lock();
            /*操作共享变量 --- 线程不安全的*/
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        /*成功在文件中获取到资源后*/
        lock.lock();
        cache.put(key, obj);
        getting.remove(key);
        references.put(key, 1);
        lock.unlock();
        return obj;
    }

    /*
    * 释放缓存
    * 计数--， 如果没有引用 就回源
    * */
    protected void release(long key){
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            /*
            * 引用数减为0
            * 移除缓存和资源计数
            * 回源
            * */
            if(ref == 0){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            }else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /*
    * 关闭资源
    * 将缓存内的资源强制回源
    * */
    //Todo:bug校验 --- 这里的逻辑不大对
    protected void close(){
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for(Long key : keys){
                /*
                * 这里的逻辑是移除缓存并且回源
                * 释放缓存的话这里的计数是--
                * 如果没有减到0呢
                * */
                release(key);
                cache.remove(key);
                references.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /*
    * 当资源不在缓存时的获取行为
    * */
    protected abstract T getFoCache(long key) throws Exception;

    /*
    * 当资源被驱逐时的写回行为 --- 回源
    * */
    protected abstract void releaseForCache(T page);

}
