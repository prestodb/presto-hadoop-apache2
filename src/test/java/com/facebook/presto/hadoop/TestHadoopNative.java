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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.io.compress.CompressionCodecFactory.getCodecClasses;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestHadoopNative
{
    @Test
    public void testNative()
    {
        HadoopNative.requireHadoopNative();

        assertTrue(NativeCodeLoader.isNativeCodeLoaded());
        assertTrue(NativeCodeLoader.buildSupportsSnappy());
        assertTrue(ZlibFactory.isNativeZlibLoaded(new Configuration()));
        assertTrue(Bzip2Factory.isNativeBzip2Loaded(new Configuration()));
    }

    @Test
    public void testCodecRoundTrip()
            throws Exception
    {
        HadoopNative.requireHadoopNative();

        Configuration conf = new Configuration();
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        for (Class<? extends CompressionCodec> clazz : getCodecClasses(conf)) {
            CompressionCodec codec = factory.getCodecByClassName(clazz.getName());
            assertNotNull(codec, clazz.getName());

            byte[] expected = "Hello world! Goodbye world!".getBytes(UTF_8);
            byte[] actual = decompress(codec, compress(codec, expected));
            assertEquals(actual, expected);
        }
    }

    private static byte[] compress(CompressionCodec codec, byte[] input)
            throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (OutputStream out = codec.createOutputStream(bytes)) {
            out.write(input);
            out.close();
        }
        return bytes.toByteArray();
    }

    private static byte[] decompress(CompressionCodec codec, byte[] input)
            throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (InputStream in = codec.createInputStream(new ByteArrayInputStream(input))) {
            int b;
            while ((b = in.read()) != -1) {
                bytes.write(b);
            }
        }
        return bytes.toByteArray();
    }
}
