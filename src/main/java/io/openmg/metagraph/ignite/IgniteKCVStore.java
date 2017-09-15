package io.openmg.metagraph.ignite;

import com.google.common.collect.Maps;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.*;

import java.util.List;
import java.util.Map;

/**
 * Created by eguoyix on 17/9/5.
 */
public class IgniteKCVStore implements KeyColumnValueStore {

    private final String storeName;

    private final IgniteCacheManager igniteCache;

    private final IgniteKCVStoreManager storeManager;


    public IgniteKCVStore(String storeName, IgniteCacheManager igniteCache, IgniteKCVStoreManager manager){
        this.storeName = storeName;
        this.igniteCache = igniteCache;
        this.storeManager = manager;
    }

    public EntryList getSlice(KeySliceQuery keySliceQuery, StoreTransaction storeTransaction) throws BackendException {
        return igniteCache.getSlice(keySliceQuery,storeTransaction);
    }

    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer,EntryList> result = Maps.newHashMap();
        for (StaticBuffer key : keys) result.put(key,getSlice(new KeySliceQuery(key,query),txh));
        return result;
    }

    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        igniteCache.mutate(key,additions,deletions,txh);
    }


    public void acquireLock(StaticBuffer staticBuffer, StaticBuffer staticBuffer1, StaticBuffer staticBuffer2, StoreTransaction storeTransaction) throws BackendException {
        throw new UnsupportedOperationException();
    }

    public KeyIterator getKeys(KeyRangeQuery keyRangeQuery, StoreTransaction txh) throws BackendException {
        return new CVIterator(keyRangeQuery, txh,igniteCache);
    }

    public KeyIterator getKeys(SliceQuery sliceQuery, StoreTransaction txh) throws BackendException {
        return new CVIterator(sliceQuery,txh,igniteCache);
    }

    public String getName() {
        return storeName;
    }

    public void close() throws BackendException {
        this.igniteCache.clear();
    }

    public void clear() {
        this.igniteCache.clear();
    }
}
