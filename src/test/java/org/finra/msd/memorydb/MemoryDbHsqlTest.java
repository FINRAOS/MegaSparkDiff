/*
 * Copyright 2017 MegaSparkDiff Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finra.msd.memorydb;

import org.junit.Assert;
import org.junit.Test;

import java.util.logging.Logger;

public class MemoryDbHsqlTest {

    @Test
    public void testInitialization()
    {
        MemoryDbHsql.getInstance().initializeMemoryDB();

        if (MemoryDbHsql.getInstance().getState() != 1)
        {
            Assert.fail("server was not running");
        }

        MemoryDbHsql.getInstance().shutdownMemoryDb();
        int state = MemoryDbHsql.getInstance().getState();
        if (state != 8)
        {
            Logger log = Logger.getLogger(this.getClass().getName());
            log.info("Memory HQL DB state was " + state);
            Assert.fail("server was running although asked to stop it");
        }
    }
}
