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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestLineReader
{
    @Test
    public void testDefaultReaderZeroBytes()
            throws IOException
    {
        byte[] input = "Hello world! Goodbye world!\n".getBytes(UTF_8);
        InputStream in = new ByteArrayInputStream(input);
        // Set the LineReader internal read buffer size 4 bytes
        LineReader reader = new LineReader(in, 4);
        Text str = new Text();
        reader.readLine(str, 0, 30);
        // There should not be any bytes read into str because the maxLineLength is 0
        assertEquals(str, new Text());
    }

    @Test
    public void testDefaultReaderNonZeroBytes()
            throws IOException
    {
        byte[] input = "Hello world! Goodbye world!\n".getBytes(UTF_8);
        InputStream in = new ByteArrayInputStream(input);
        // Set the LineReader internal read buffer size 4 bytes
        LineReader reader = new LineReader(in, 4);
        Text str = new Text();
        reader.readLine(str, 30, 30);
        // The LineReader does not store the new line character into str,
        // so we need to compare just the first 27 characters.
        assertEquals(str, new Text("Hello world! Goodbye world!".getBytes(UTF_8)));
    }

    @Test
    public void testCustomReaderZeroBytes()
            throws IOException
    {
        byte[] input = "Hello world! Goodbye world!\n".getBytes(UTF_8);
        byte[] delimiter = "!".getBytes(UTF_8);
        InputStream in = new ByteArrayInputStream(input);
        // Set the LineReader internal read buffer size 4 bytes
        LineReader reader = new LineReader(in, 4, delimiter);
        Text str = new Text();
        reader.readLine(str, 0, 30);
        // There should not be any bytes read into str because the maxLineLength is 0
        assertEquals(str, new Text());
    }

    @Test
    public void testCustomReaderNonZeroBytes()
            throws IOException
    {
        byte[] input = "Hello world! Goodbye world!\n".getBytes(UTF_8);
        byte[] delimiter = "!".getBytes(UTF_8);
        InputStream in = new ByteArrayInputStream(input);
        // Set the LineReader internal read buffer size 4 bytes
        LineReader reader = new LineReader(in, 4, delimiter);
        Text str = new Text();
        reader.readLine(str, 30, 30);
        // The first 11 bytes of input should be read into str because the 12th character is the delimiter
        assertEquals(str, new Text("Hello world".getBytes(UTF_8)));
    }

    @Test
    public void testDefaultReaderMaxBytesConsumed()
            throws IOException
    {
        byte[] input = "Hello world! Goodbye world!\n".getBytes(UTF_8);
        InputStream in = new ByteArrayInputStream(input);
        // Set the LineReader internal read buffer size 4 bytes
        LineReader reader = new LineReader(in, 4);
        Text str = new Text();
        try {
            // The read should fail because the line length is greater than the maxBytesToConsume
            reader.readLine(str, 0, 10);
            fail("Expected exception");
        }
        catch (TextLineLengthLimitExceededException e) {
            // It should be 3 reads of 4 bytes each, so the final bytesConsumed is 12
            assertEquals(e.getMessage(), "Too many bytes before newline: 12");
        }
    }

    @Test
    public void testDefaultReaderMaxLineLength()
            throws IOException
    {
        byte[] input = "Hello world! Goodbye world!\n".getBytes(UTF_8);
        InputStream in = new ByteArrayInputStream(input);
        // Set the LineReader internal read buffer size 4 bytes
        LineReader reader = new LineReader(in, 4);
        Text str = new Text();
        try {
            // The read should fail because the line length is greater than the maxLineLength
            reader.readLine(str, 10, 100);
            fail("Expected exception");
        }
        catch (TextLineLengthLimitExceededException e) {
            assertEquals(e.getMessage(), "Too many bytes before newline: 10");
        }
    }

    @Test
    public void testCustomReaderMaxBytesConsumed()
            throws IOException
    {
        byte[] input = "Hello world! Goodbye world!\n".getBytes(UTF_8);
        byte[] delimiter = "!".getBytes(UTF_8);
        InputStream in = new ByteArrayInputStream(input);
        // Set the LineReader internal read buffer size 4 bytes
        LineReader reader = new LineReader(in, 4, delimiter);
        Text str = new Text();
        try {
            // The read should fail because the line length is greater than the maxBytesToConsume
            reader.readLine(str, 0, 5);
            fail("Expected exception");
        }
        catch (TextLineLengthLimitExceededException e) {
            // It should be 2 reads of 4 bytes each, so the final bytesConsumed is 8
            assertEquals(e.getMessage(), "Too many bytes before delimiter: 8");
        }
    }

    @Test
    public void testCustomReaderMaxLineLength()
            throws IOException
    {
        byte[] input = "Hello world! Goodbye world!\n".getBytes(UTF_8);
        byte[] delimiter = "!".getBytes(UTF_8);
        InputStream in = new ByteArrayInputStream(input);
        // Set the LineReader internal read buffer size 4 bytes
        LineReader reader = new LineReader(in, 4, delimiter);
        Text str = new Text();
        try {
            // The read should fail because the line length is greater than the maxLineLength
            reader.readLine(str, 10, 100);
            fail("Expected exception");
        }
        catch (TextLineLengthLimitExceededException e) {
            assertEquals(e.getMessage(), "Too many bytes before delimiter: 10");
        }
    }
}
