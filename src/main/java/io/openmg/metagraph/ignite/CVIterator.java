package io.openmg.metagraph.ignite;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.util.RecordIterator;

import java.io.IOException;

/**
 * Created by eguoyix on 17/9/7.
 */
public class CVIterator implements KeyIterator {



    public RecordIterator<Entry> getEntries() {
        return null;
    }

    public void close() throws IOException {

    }

    public boolean hasNext() {
        return false;
    }

    public StaticBuffer next() {
        return null;
    }

    public void remove() {

    }
}
