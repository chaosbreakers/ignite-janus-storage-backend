package io.openmg.metagraph.ignite;

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

    public EntryList getEntryList(K key){
        EntryList result = EntryArrayList.of(new ArrayList<Entry>());
        Map o = (Map)cache.get(key);
        if(o ==null){
            return EntryList.EMPTY_LIST;
        }
        ArrayList temp = new ArrayList();
        for(Object e : o.values()){
            temp.add((Entry) e);
        }
        result = EntryArrayList.of(temp);
        return result;
    }

    public void mutate(K key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh){
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
            EntryList olddata = getEntryList(key);

            int oldsize = olddata.size();
            Entry[] newdata = new Entry[oldsize + add.length];

            //Merge sort
            int i = 0, iold = 0, iadd = 0, idel = 0;
            while (iold < oldsize) {
                Entry e = olddata.get(iold);
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
            saveNewData(key,newdata);
        } finally {
            lock.unlock();
        }
    }

    public void saveNewData(K key, Entry[] newdata) {
        Object o = cache.get(key);
        if(o == null){
            insertNewRecord(key,newdata);
        }else {
            updateRecord(key,newdata);
        }
    }

    private void updateRecord(K key, Entry[] newdata) {
        cache.remove(key);
        insertNewRecord(key,newdata);
    }

    private void insertNewRecord(K key, Entry[] newdata) {
        Map cvMap = Maps.newLinkedHashMap();

        for(Entry cv: newdata){
            StaticBuffer column = cv.getColumn();
            cvMap.put(column,cv);
        }
        keys.add(key);
        cache.put(key,cvMap);
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
        Lock lock = getLock(txh);
        lock.lock();
        try {
            StaticBuffer key = query.getKey();
            LinkedHashMap o = (LinkedHashMap) cache.get(key);
            if(o ==null){
                return EntryList.EMPTY_LIST;
            }
            StaticBuffer sliceStart = query.getSliceStart();
            StaticBuffer sliceEnd = query.getSliceEnd();

            o.get(sliceStart);
            EntryList result = EntryArrayList.of(new ArrayList<Entry>());
            Set set = o.entrySet();
            boolean started = false;
            for(Object item:set){
                Map.Entry entry = (Map.Entry)item;
                Object column = entry.getKey();
                if(column.equals(sliceStart)){
                    started = true;
                }
                if(started){
                    result.add((Entry) entry.getValue());
                }
                if(column.equals(sliceEnd)){
                    started = false;
                    break;
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

}
