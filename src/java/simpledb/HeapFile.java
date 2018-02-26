package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    private File f;
    private TupleDesc td;
    private ArrayList<Page> dirtypages;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f           = f;
        this.td          = td;
        this.dirtypages  = new ArrayList<Page>();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        //return null;
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    // @throws IllegalArgumentException if the page does not exist in this file.
    public Page readPage(PageId pid) {
        // some code goes here
        //return null;
        // check whether the page exists in this file
        if (pid.getPageNumber() >= numPages())
           throw new IllegalArgumentException();
        //System.out.println("readPage:"+pid.getPageNumber());
        HeapPageId hpId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
        HeapPage pg;
        // read the data from File f to byte[] buf
        byte[] buf = new byte[(int) f.length()];
        //System.out.println(f.length());
        int offset = pid.getPageNumber() * Database.getBufferPool().getPageSize();
        int count  = 0;
        try {
            InputStream is  = new FileInputStream(f);
            is.skip(offset);
            while (offset < buf.length
                   && (count = is.read(buf, 0, buf.length - offset)) >= 0) {
                offset += count;
            }
            is.close();
            pg     = new HeapPage(hpId, buf);
            //if (count <= 4096) {
            //    System.out.println("not first page of file");
            //    Iterator<Tuple> it = pg.iterator();
            //    while(it.hasNext()) {
            //        System.out.println("ddd:"+it.next());
            //    }
            //}
        } catch (IOException e) {
            return null;
        }
        return pg;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        //System.out.println(pageno);
        //FileOutputStream os = new FileOutputStream(f, true);
        RandomAccessFile fs = new RandomAccessFile(f, "rw");
        int offset      = pid.getPageNumber() * Database.getBufferPool().getPageSize();
        byte[] pageData = page.getPageData();
        //for (byte b: pageData)
        //    System.out.println(b);
        fs.seek(offset);
        fs.write(pageData);
        fs.close();
    }

    /**
     * Returns the number of pags in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //return 0;
        return (int) Math.ceil(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        int n              = numPages();
        int tableid        = getId();
        BufferPool bp      = Database.getBufferPool();
        boolean insertDone = false;

        for (int i = 0; i < n; i++) {
            HeapPageId hpId = new HeapPageId(tableid, i);
            HeapPage hp     = (HeapPage) bp.getPage(tid, hpId, Permissions.READ_WRITE);
            if (hp.getNumEmptySlots() != 0) {
                hp.insertTuple(t);
                insertDone = true;
                dirtypages.add(hp);
                break;
            }
        }

        //if all pages are full, append a new page
        if (!insertDone) {
            HeapPageId nhpId = new HeapPageId(tableid, n);
            HeapPage nhp     = new HeapPage(nhpId, HeapPage.createEmptyPageData());
            writePage(nhp);
            nhp              = (HeapPage) bp.getPage(tid, nhpId, Permissions.READ_WRITE);
            nhp.insertTuple(t);
            dirtypages.add(nhp);
        }

        return dirtypages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        HeapPageId hpId = (HeapPageId) t.getRecordId().getPageId();
        BufferPool bp = Database.getBufferPool();
        HeapPage hp = (HeapPage) bp.getPage(tid, hpId, Permissions.READ_WRITE);
        hp.deleteTuple(t);
        //System.out.println(t.getRecordId().getPageId().hashCode());
        //System.out.println("d:"+hp.getNumEmptySlots());
        //System.out.println("heapfile:"+bp);

        dirtypages.add(hp);
        return dirtypages;
    }

    private class HeapFileIterator extends AbstractDbFileIterator {
        private int pageno;
        private TransactionId tid;
        private BufferPool bp;
        Iterator<Tuple> it;
        
        protected HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (it.hasNext()) {
                return it.next();
            }
            else {
                if (pageno < numPages() - 1)
                    pageno++;
                else
                    return null;
                HeapPageId hpId = new HeapPageId(getId(), pageno);
                HeapPage hp = (HeapPage) bp.getPage(tid, hpId, Permissions.READ_ONLY);
                it = hp.iterator();
                return it.next();
            }
        }

        public void rewind() throws DbException, TransactionAbortedException {
            pageno  = 0;
        }
        
        // open the iterator
        public void open() throws DbException, TransactionAbortedException {
            bp     = Database.getBufferPool();
            pageno = 0;
            HeapPageId hpId = new HeapPageId(getId(), pageno);
            HeapPage hp = (HeapPage) bp.getPage(tid, hpId, Permissions.READ_ONLY);
            it = hp.iterator();
        }
        // close the iterator
        public void close() {
            super.close();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

