package io.openmg.metagraph.ignite.graphdb.database.management;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPerformanceMemoryTest;

import io.openmg.metagraph.ignite.IgniteStorageSetup;

/**
 * Created by shaohuahe on 2017/9/20.
 */
public class IgniteGraphPerformanceMemoryTest extends JanusGraphPerformanceMemoryTest {
    @Override
    public WriteConfiguration getConfiguration() {
        return IgniteStorageSetup.getIgniteWriteConfiguration();
    }
}
