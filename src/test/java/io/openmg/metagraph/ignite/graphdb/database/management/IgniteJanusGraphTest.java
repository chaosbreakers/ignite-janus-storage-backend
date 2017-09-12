package io.openmg.metagraph.ignite.graphdb.database.management;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;

import io.openmg.metagraph.ignite.IgniteStorageSetup;

/**
 * Created by shaohuahe on 2017/9/12.
 */
public class IgniteJanusGraphTest extends JanusGraphTest {
    @Override
    public WriteConfiguration getConfiguration() {
        return IgniteStorageSetup.getIgniteWriteConfiguration();
    }
}
