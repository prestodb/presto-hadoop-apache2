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
package org.apache.hadoop.fs;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.EnumSet;

public class PrestoFilterFileSystemWrapper
        extends FilterFileSystem
{
    public PrestoFilterFileSystemWrapper(FileSystem fs)
    {
        super(fs);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize)
            throws IOException
    {
        return new InputStreamWrapper(getRawFileSystem().open(f, bufferSize), this);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException
    {
        return new OutputStreamWrapper(getRawFileSystem().append(f, bufferSize, progress), this);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return new OutputStreamWrapper(getRawFileSystem().create(f, permission, overwrite, bufferSize, replication, blockSize, progress), this);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
            throws IOException
    {
        return new OutputStreamWrapper(getRawFileSystem().create(f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt), this);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return new OutputStreamWrapper(getRawFileSystem().createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress), this);
    }
}
