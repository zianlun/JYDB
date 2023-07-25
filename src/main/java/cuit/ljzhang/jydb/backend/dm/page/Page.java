package cuit.ljzhang.jydb.backend.dm.page;

/**
 * @InterfaceName Page
 * @Description
 * @Author ljzhang
 * @Date 2023/7/25 20:09
 * @Version 1.0
 *
 */
public interface Page {

    void lock();

    void unlock();

    void release();

    void setDirty(boolean isDirty);

    boolean isDirty();

    int getPageNumber();

    byte[] getData();

}
