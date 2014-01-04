/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.Test;

import java.net.URI;

import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

public class TestHadoopFileSystemCache
{
    @Test
    public void testCache()
            throws Exception
    {
        HadoopFileSystemCache.initialize();

        FileSystem.closeAll();

        Configuration conf = new Configuration();
        URI uri = URI.create("file:///");

        FileSystem fs1 = FileSystem.get(uri, conf);
        FileSystem fs2 = FileSystem.get(uri, conf);
        assertSame(fs2, fs1);

        FileSystem.closeAll();

        FileSystem fs3 = FileSystem.get(uri, conf);
        assertNotSame(fs3, fs1);
    }
}
