package cuit.ljzhang.jydb.backend.vm;

import cuit.ljzhang.jydb.backend.tm.TransactionManager;

/**
 * @ClassName Visibility
 * @Description
 * @Author ljzhang
 * @Date 2023/8/7 21:20
 * @Version 1.0
 */
public class Visibility {

    /*
     *是否发生了版本跳跃判断
     * 版本跳跃：当前事务需要修改的x被不可见的事务修改了
     * */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            /*
             * 不可见事务（可重复读的情况下）：
             *       Ti > Tj
             *       Tj在Ti事务开始时是活跃的
             * */
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    /*
     *记录对当前事务是否可见
     * t.level:事务的隔离级别
     * */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            /*读已提交*/
            return readCommitted(tm, t, e);
        } else {
            /*读未提交*/
            return repeatableRead(tm, t, e);
        }
    }

    /*
    * 逻辑解析
    *      xmin -- 创建该记录的事务编号
    *      xmax -- 删除该记录的事务编号
    *      xid -- 当前查询记录的事务编号
    *      xid == xmin && xmax == 0 可见
    *      xid != xmin
    *            xmin 未提交  不可见
    *            xin  提交
    *                xmax == 0 可见 --- 表示记录未删除
    *                xmax != 0
    *                       xmax != xid
    *                            xmax 尚未提交  可见
    *                       否则 不可见 (两种情况：在当前事务删除或者删除事务已经被提交)
    * */
    public static boolean readCommitted(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xid == xmin && xmax == 0)
            return true;
        if(tm.isCommitted(xmin)){
            if(xmax == 0) return true;
            if(xmax != xid){
                if(!tm.isCommitted(xmax))
                    return true;
            }
        }
        return false;
    }

    /*
    * 逻辑解析
    *          xmin == xid && xmax == 0 可见
    *          xmin 已经提交 & xmin < xid & 在xid开始前xmin已提交
    *               xmax == 0 尚未删除   可见
    *               xmax != xid
    *                     xmax 尚未提交  可见
    *                     xmax > xid  当前事务开始之前xmax尚未创建 可见
    *                     xmax 在 xid 开始时尚未提交   可见
    * */
    public static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmax = e.getXmax();
        long xmin = e.getXmin();
        if(xmin == xid && xmax == 0)
            return true;
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)){
            if(xmax == 0)
                return true;
            if(xmax != xid){
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax))
                    return true;
            }
        }
        return false;
    }
}
