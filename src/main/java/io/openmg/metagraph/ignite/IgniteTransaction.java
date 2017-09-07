package io.openmg.metagraph.ignite;

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

/**
 * Created by eguoyix on 17/9/6.
 */
public class IgniteTransaction extends AbstractStoreTransaction {
    public IgniteTransaction(BaseTransactionConfig config) {
        super(config);
    }
}
