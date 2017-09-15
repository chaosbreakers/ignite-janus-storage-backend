package io.openmg.metagraph.ignite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.NoLock;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;

import java.net.URL;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL;

/**
 * Created by eguoyix on 17/9/6.
 */
public class IgniteCacheManager<K,C,V> {

    private static volatile boolean igniteInited = false;

    private static final double SIZE_THRESHOLD = 0.66;

    private static  Ignite ignite = null;

    private static ArrayList keys = new ArrayList();

    private Object initLock = new Object();
    public  IgniteCacheManager(String name){
        synchronized (initLock){
            if(!igniteInited){
                ignite = Ignition.start("ignite-default-config.xml");
                ignite.active(true);
                igniteInited = true;
            }
        }
        this.cache = ignite.getOrCreateCache(name);
    }


    private IgniteCache cache;

    public Data getEntryList(K key){
        Data o = (Data)cache.get(key);
        if(o==null){
            o = new Data(new Entry[0], 0);
        }
        return o;
    }

    public void clear(){
//        this.cache.clear();
    }

    public synchronized void mutate(K key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh){
        //Prepare data
        Entry[] add;
        if (!additions.isEmpty()) {
            add = new Entry[additions.size()];
            int pos = 0;
            for (Entry e : additions) {
                add[pos] = e;
                pos++;
            }
            Arrays.sort(add);
        } else add = new Entry[0];

        //Filter out deletions that are also added
        Entry[] del;
        if (!deletions.isEmpty()) {
            del = new Entry[deletions.size()];
            int pos=0;
            for (StaticBuffer deletion : deletions) {
                Entry delEntry = StaticArrayEntry.of(deletion);
                if (Arrays.binarySearch(add,delEntry) >= 0) continue;
                del[pos++]=delEntry;
            }
            if (pos<deletions.size()) del = Arrays.copyOf(del,pos);
            Arrays.sort(del);
        } else del = new Entry[0];

        //performance issue might be here
        Lock lock = getLock(txh);
        lock.lock();
        try {
            Data oldData = getEntryList(key);
            Entry[] olddata = oldData.array;

            int oldsize = oldData.getNoneNullEntrySize();
            Entry[] newdata = new Entry[oldsize + add.length];

            //Merge sort
            int i = 0, iold = 0, iadd = 0, idel = 0;
            while (iold < oldsize) {
                Entry e = olddata[iold];
                iold++;
                //Compare with additions
                if (iadd < add.length) {
                    int compare = e.compareTo(add[iadd]);
                    if (compare >= 0) {
                        e = add[iadd];
                        iadd++;
                        //Skip duplicates
                        while (iadd < add.length && e.equals(add[iadd])) iadd++;
                    }
                    if (compare > 0) iold--;
                }
                //Compare with deletions
                if (idel < del.length) {
                    int compare = e.compareTo(del[idel]);
                    if (compare == 0) e = null;
                    if (compare >= 0) idel++;
                }
                if (e != null) {
                    newdata[i] = e;
                    i++;
                }
            }
            while (iadd < add.length) {
                newdata[i] = add[iadd];
                i++;
                iadd++;
            }

            if (i * 1.0 / newdata.length < SIZE_THRESHOLD) {
                //shrink array to free space
                Entry[] tmpdata = newdata;
                newdata = new Entry[i];
                System.arraycopy(tmpdata, 0, newdata, 0, i);
            }
            Data saveData = new Data(newdata,i);
            saveNewData(key,saveData);
        } finally {
            lock.unlock();
        }
    }

    public void saveNewData(K key, Data data) {
        Object o = cache.get(key);
        if(o == null){
            insertNewRecord(key,data);
        }else {
            updateRecord(key,data);
        }
    }

    private void updateRecord(K key, Data newdata) {
        cache.remove(key);
        insertNewRecord(key,newdata);
    }

    private void insertNewRecord(K key, Data newdata) {
//        Map cvMap = Maps.newLinkedHashMap();
//        for(Entry cv: newdata){
//            if(cv!=null){
//                StaticBuffer column = cv.getColumn();
//                cvMap.put(column,cv);
//            }
//        }
        keys.add(key);
        cache.put(key,newdata);
    }

    private ReentrantLock lock = null;
    private Lock getLock(StoreTransaction txh) {
        Boolean txOn = txh.getConfiguration().getCustomOption(STORAGE_TRANSACTIONAL);
        if (null != txOn && txOn) {
            if (lock == null) {
                synchronized (this) {
                    if (lock == null) {
                        lock = new ReentrantLock();
                    }
                }
            }
            return lock;
        } else return NoLock.INSTANCE;
    }

    public List getKeys(){
        return keys;
    }

    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh){
//        Lock lock = getLock(txh);
//        lock.lock();
//        try {
//            StaticBuffer key = query.getKey();
//            LinkedHashMap o;
//            String cacheName = cache.getName();
//            if(cache.get(key) ==null){
//                return EntryList.EMPTY_LIST;
//            }else {
//                o = (LinkedHashMap) cache.get(key);
//            }
//            StaticBuffer sliceStart = query.getSliceStart();
//            StaticBuffer sliceEnd = query.getSliceEnd();
//
//            o.get(sliceStart);
//            EntryList result = EntryArrayList.of(new ArrayList<Entry>());
//            Set set = o.entrySet();
//            boolean started = false;
//            for(Object item:set){
//                Map.Entry entry = (Map.Entry)item;
//                Object column = entry.getKey();
//                if(column.equals(sliceStart)){
//                    started = true;
//                }
//                if(started){
//                    result.add((Entry) entry.getValue());
//                }
//                if(column.equals(sliceEnd)){
//                    started = false;
//                    break;
//                }
//            }
//            return result;
//        } finally {
//            lock.unlock();
//        }
        Lock lock = getLock(txh);
        lock.lock();
        try {
            Data datacp = getEntryList((K)query.getKey());
            int start = datacp.getIndex(query.getSliceStart());
            if (start < 0) start = (-start - 1);
            int end = datacp.getIndex(query.getSliceEnd());
            if (end < 0) end = (-end - 1);
            if (start < end) {
                MemoryEntryList result = new MemoryEntryList(end - start);
                for (int i = start; i < end; i++) {
                    if (query.hasLimit() && result.size() >= query.getLimit()) break;
                    result.add(datacp.get(i));
                }
                return result;
            } else {
                return EntryList.EMPTY_LIST;
            }
        } finally {
            lock.unlock();
        }

    }

    private static class Data {

        final Entry[] array;
        final int size;

        Data(final Entry[] array, final int size) {
            Preconditions.checkArgument(size >= 0 && size <= array.length);
            assert isSorted();
            this.array = array;
            this.size = size;
        }

        int getNoneNullEntrySize(){
            int size = 0;
            for(Entry e :array){
                if(e!=null){
                    size ++;
                }
            }
            return size;
        }

        boolean isEmpty() {
            return size == 0;
        }

        int getIndex(StaticBuffer column) {
            return Arrays.binarySearch(array, 0, size, StaticArrayEntry.of(column));
        }

        Entry get(int index) {
            return array[index];
        }

        boolean isSorted() {
            for (int i = 1; i < size; i++) {
                if (!(array[i].compareTo(array[i - 1]) > 0)) return false;
            }
            return true;
        }

    }


    private static class MemoryEntryList extends ArrayList<Entry> implements EntryList {

        public MemoryEntryList(int size) {
            super(size);
        }

        @Override
        public Iterator<Entry> reuseIterator() {
            return iterator();
        }

        @Override
        public int getByteSize() {
            int size = 48;
            for (Entry e : this) {
                size += 8 + 16 + 8 + 8 + e.length();
            }
            return size;
        }
    }

}
