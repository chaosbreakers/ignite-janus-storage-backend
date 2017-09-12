package io.openmg.metagraph.ignite;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by eguoyix on 17/9/7.
 */
public class CVIterator implements KeyIterator {

    private final Iterator<StaticBuffer> rows;
    private final SliceQuery columnSlice;
    private final StoreTransaction transaction;

    private StaticBuffer currentRow;
    private StaticBuffer nextRow;
    private boolean isClosed;

    private IgniteCacheManager igniteCacheManager;

    public CVIterator(@Nullable SliceQuery columns,
                       final StoreTransaction transaction,IgniteCacheManager igniteCacheManager) {
        if(columns instanceof KeyRangeQuery){
            Iterator iterator = extractKeyRange((KeyRangeQuery) columns, igniteCacheManager);
            rows = iterator;
        }else {
            rows = igniteCacheManager.getKeys().iterator();
        }
        this.columnSlice = columns;
        this.transaction = transaction;
        this.igniteCacheManager = igniteCacheManager;
    }

    @NotNull
    private Iterator extractKeyRange(@Nullable KeyRangeQuery columns, IgniteCacheManager igniteCacheManager) {
        List keys = igniteCacheManager.getKeys();
        StaticBuffer keyStart = columns.getKeyStart();
        StaticBuffer keyEnd = columns.getKeyEnd();
        ArrayList keyRange = new ArrayList();
        boolean started = false;
        for(Object item:keys){
            if(item.equals(keyStart)){
                started = true;
            }
            if(started){
                keyRange.add(item);
            }
            if(item.equals(keyEnd)){
                started = false;
                break;
            }
        }
        return keyRange.iterator();
    }

    @Override
    public RecordIterator<Entry> getEntries() {
        ensureOpen();

        if (columnSlice == null)
            throw new IllegalStateException("getEntries() requires SliceQuery to be set.");

        final KeySliceQuery keySlice = new KeySliceQuery(currentRow, columnSlice);
        return new RecordIterator<Entry>() {
            private final Iterator<Entry> items = igniteCacheManager.getSlice(keySlice,transaction).iterator();

            @Override
            public boolean hasNext() {
                ensureOpen();
                return items.hasNext();
            }

            @Override
            public Entry next() {
                ensureOpen();
                return items.next();
            }

            @Override
            public void close() {
                isClosed = true;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Column removal not supported");
            }
        };
    }

    @Override
    public boolean hasNext() {
        ensureOpen();

        if (null != nextRow)
            return true;

        while (rows.hasNext()) {
            nextRow = rows.next();
            final KeySliceQuery keySlice = new KeySliceQuery(nextRow, columnSlice);
            EntryList ents = igniteCacheManager.getSlice(keySlice, transaction);
            if (null != ents && 0 < ents.size())
                break;
        }

        return null != nextRow;
    }

    @Override
    public StaticBuffer next() {
        ensureOpen();

        Preconditions.checkNotNull(nextRow);

        currentRow = nextRow;
        nextRow = null;
        ;

        return currentRow;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    private void ensureOpen() {
        if (isClosed)
            throw new IllegalStateException("Iterator has been closed.");
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Key removal not supported");
    }

}
