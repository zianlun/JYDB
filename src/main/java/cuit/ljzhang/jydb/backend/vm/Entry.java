package cuit.ljzhang.jydb.backend.vm;

import com.google.common.primitives.Bytes;
import cuit.ljzhang.jydb.backend.common.SubArray;
import cuit.ljzhang.jydb.backend.dm.dataItem.DataItem;
import cuit.ljzhang.jydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * @ClassName Entry
 * @Description
 * @Author ljzhang
 * @Date 2023/7/31 22:26
 * @Version 1.0
 * entry:
 *      ---  维护记录的结构
 *      --- 一条记录只有一个版本
 * 根据前两个字段来判断一个可见性和可重复读
 * [XMIN][XMAX][DATA]
 */
public class Entry {

    /*数据的偏移*/
    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;


    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    /*
    * 加载entry
    * */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem dataItem = ((VersionManagerImpl)(vm)).dm.read(uid);
        return newEntry(vm, dataItem, uid);
    }

    /*
    * 将数据包装为一个记录
    * 然后返回
    * */
    public static byte[] wrapEntryRaw(long xid, byte[] data){
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    /*以拷贝的形式返回内容*/
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }

    public long getUid() {
        return uid;
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /*设置删除该记录的事务id*/
    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }
}
