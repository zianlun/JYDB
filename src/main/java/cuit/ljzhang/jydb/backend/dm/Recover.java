package cuit.ljzhang.jydb.backend.dm;

import com.google.common.primitives.Bytes;
import cuit.ljzhang.jydb.backend.common.SubArray;
import cuit.ljzhang.jydb.backend.dm.dataItem.DataItem;
import cuit.ljzhang.jydb.backend.dm.logger.Logger;
import cuit.ljzhang.jydb.backend.dm.page.Page;
import cuit.ljzhang.jydb.backend.dm.page.PageX;
import cuit.ljzhang.jydb.backend.dm.pageCache.PageCache;
import cuit.ljzhang.jydb.backend.tm.TransactionManager;
import cuit.ljzhang.jydb.backend.utils.Panic;
import cuit.ljzhang.jydb.backend.utils.Parser;
import java.util.Map.Entry;
import java.util.*;

/**
 * @ClassName Recover
 * @Description
 * @Author ljzhang
 * @Date 2023/7/27 16:44
 * @Version 1.0
 * 规定：
 *      1.正在进行的事务，不会读取其他任何未提交的事务产生的数据
 *          ---解决级联回滚
 *      2.正在进行的事务，不会修改任何其他未提交的事务修改或产生的数据
 *          ---解决多线程下，恢复出错的情况
 *              ---总结：不支持读未提交
 * 日志恢复类似于statement：仅仅记录了日志的前相和后相
 *      ---这种日志恢复策略被称为逻辑日志
 *      ---有问题，需要进入更多的日志种类，不好实现
 *      ---采用规定的形式，才有了上面的两个规定
 * 日志恢复策略：
 *      1.重做所有崩溃时已完成(commit或aborted)的事务
 *      2.撤销所有崩溃时未完成(active)的事务
 * 日志格式【对应日志文件中的data字段】
 *      1.[LogType][XID][UID][OldRaw][NewRaw]   更新日志
 *      2.[LogType][XID][PageNumber][Offset][Raw]    插入日志
 */
public class Recover {
    /*两种日志操作类型*/
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    /*回滚和重做*/
    private static final int REDO = 0;
    private static final int UNDO = 1;

    /*
    * 数据结构：[LogType][XID][UID][OldRaw][NewRaw]
    * 字节数：     1      8     8
    * */
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    /*更新日志*/
    static class UpdateLogInfo {
        long xid;
        int pageNumber;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /*
    * 数据结构：[LogType][XID][PageNumber][Offset][Raw]
    * 字节数：     1      8        4         2
    * */
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    /*插入日志*/
    static class InsertLogInfo {
        long xid;
        int pageNumber;
        short offset;
        byte[] raw;
    }

    /*当数据文件校验不通过时 执行日志恢复数据策略*/
    public static void recover(TransactionManager tm, Logger logger, PageCache pc){
        System.out.println("recover...");
        logger.rewind();
        int maxPageNumber = 0;
        /*找到所有操作记录中的一个最大的操作页  */
        while(true){
            byte[] log = logger.next();
            if(log == null)break;
            int pageNumber;
            if(isInsertLog(log)){
                InsertLogInfo iLog = parseInsertLog(log);
                pageNumber = iLog.pageNumber;
            }else{
                UpdateLogInfo uLog = parseUpdateLog(log);
                pageNumber = uLog.pageNumber;
            }
            if(pageNumber > maxPageNumber){
                maxPageNumber = pageNumber;
            }
        }
        /*第1页有重要用途 不包括在内*/
        if(maxPageNumber == 0){
            maxPageNumber = 1;
        }
        /* 截断最大记录页之后的数据   */
        pc.truncateByBgno(maxPageNumber);
        System.out.println("Truncate to " + maxPageNumber + " pages.");

        redoTranscations(tm, logger, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, logger, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /*
    * TransactionManager:维护了事务的状态
    * Logger:操作日志文件
    * PageCache:页面缓存管理
    * 日志重做操作
    * */
    private static void redoTranscations(TransactionManager tm,
                                         Logger logger,
                                         PageCache pc){
        logger.rewind();
        while(true){
            byte[] log = logger.next();
            if(log == null)break;
            if(isInsertLog(log)){
                InsertLogInfo iLog = parseInsertLog(log);
                long xId = iLog.xid;
                if(!tm.isActive(xId)){
                    doInsertLog(pc, log, REDO);
                }
            }else{
                UpdateLogInfo xId = parseUpdateLog(log);
            }
        }
    }

    /* 根据回滚操作 */
    private static void undoTranscations(TransactionManager tm,
                                         Logger logger,
                                         PageCache pc){
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        /*遍历日志*/
        logger.rewind();
        while(true){
            byte[] log = logger.next();
            if(log == null)break;
            if(isInsertLog(log)){
                InsertLogInfo iLog = parseInsertLog(log);
                long xid = iLog.xid;
                if(tm.isActive(xid)){
                    logCache.put(xid, new ArrayList<>());
                }
                logCache.get(xid).add(log);
            }else{
                UpdateLogInfo uLod = parseUpdateLog(log);
                long xid = uLod.xid;
                if(tm.isActive(xid)){
                    logCache.put(xid, new ArrayList<>());
                }
                logCache.get(xid).add(log);
            }
        }
        /*
        * 遍历键值对中的集合
        * 逆序处理日志
        * */
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()){
            List<byte[]> logs = entry.getValue();
            /*回滚日志应该逆序处理*/
            for(int i = logs.size() - 1; i >= 0; i--){
                byte[] log = logs.get(i);
                if(isInsertLog(log)){
                    doInsertLog(pc, log, UNDO);
                }else{
                    doUpdateLog(pc, log, UNDO);
                }
            }
            /*取消事务 也就是事务执行失败回滚后的标记操作*/
            tm.abort(entry.getKey());
        }
    }

    /*判断日志类型是否为插入*/
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /*解析byte[]为插入日志*/
    private static InsertLogInfo parseInsertLog(byte[] log){
        InsertLogInfo iLog = new InsertLogInfo();
        iLog.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        iLog.pageNumber = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        iLog.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        iLog.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return iLog;
    }

    /*对插入日志进行的一个重做或者回滚的操作*/
    private static void doInsertLog(PageCache pc,
                                    byte[] log,
                                    int flag){
        InsertLogInfo iLog = parseInsertLog(log);
        Page page = null;
        try {
            /*根据page编号获取到缓存页*/
            page = pc.getPage(iLog.pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }
        //Todo:待完善
        if(flag == UNDO){

        }
    }

    /*将字节数组解析为 UpdateLogInfo*/
    private static UpdateLogInfo parseUpdateLog(byte[] log){
        UpdateLogInfo uLog = new UpdateLogInfo();
        uLog.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        /*
        * uId: 由偏移量 + pageNumber组成
        *       偏移量为低位的16位
        *       pageNumber位高位的四个字节  --- 也就是32位
        * */
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        uLog.offset = (short)(uid & ((1L << 16) - 1));
        /*无符号位右移*/
        uid >>>= 32;
        uLog.pageNumber = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        uLog.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        uLog.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return uLog;
    }

    /*updateLog 的回滚和重做操作*/
    private static void doUpdateLog(PageCache pc, byte[] log, int flag ){
        int pageNumber;
        short offset;
        byte[] raw;
        if(flag == REDO){
            UpdateLogInfo uLog = parseUpdateLog(log);
            pageNumber = uLog.pageNumber;
            offset = uLog.offset;
            raw = uLog.newRaw;
        }else{
            UpdateLogInfo uLog = parseUpdateLog(log);
            pageNumber = uLog.pageNumber;
            offset = uLog.offset;
            raw = uLog.oldRaw;
        }
        Page page = null;
        try {
            page = pc.getPage(pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(page, raw, offset);
        } finally {
            page.release();
        }
    }

    /*插入日志的生成*/
    public static byte[] insertLog(long xid, Page page, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(page));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    /*更新日志的生成*/
    public static byte[] updateLog(long xid, DataItem dataItem) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }
}
