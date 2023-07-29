package cuit.ljzhang.jydb.backend.dm.pageCache;

import cuit.ljzhang.jydb.backend.dm.page.Page;
import cuit.ljzhang.jydb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static cuit.ljzhang.jydb.common.Error.*;

/**
 * @ClassName PageCache
 * @Description
 * @Author ljzhang
 * @Date 2023/7/25 20:11
 * @Version 1.0
 *
 */
//Todo: 待完成 + 注释
public interface PageCache {

    //页面大小为 8k  --- 2^13字节
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pageNumber) throws Exception;

    void close();

    void release(Page page);

    void truncateByBgno(int maxPagno);

    int getPageNumbers();

    void flushPage(Page page);

    static PageCacheImpl create(String path, long memory){
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }

    static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory / PAGE_SIZE);
    }

}
