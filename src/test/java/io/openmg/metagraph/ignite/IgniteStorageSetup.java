/*
 *
 *   Copyright (C) 2015-2017 Monogram Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package io.openmg.metagraph.ignite;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

/**
 * @author Ranger Tsao(https://github.com/boliza)
 */
public class IgniteStorageSetup extends StorageSetup {

    public static ModifiableConfiguration getIgniteConfiguration() {
        return buildGraphConfiguration().set(STORAGE_BACKEND, "io.openmg.metagraph.ignite.IgniteKCVStoreManager");
    }

    public static WriteConfiguration getIgniteWriteConfiguration() {
        return getIgniteConfiguration().getConfiguration();
    }
}
