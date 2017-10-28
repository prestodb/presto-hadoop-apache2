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

import com.google.common.collect.ImmutableSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FileSystem.getFileSystemClass;
import static org.apache.hadoop.security.UserGroupInformationShim.getSubject;

public final class PrestoFileSystemCache
        extends FileSystem.Cache
{
    public static final Log log = LogFactory.getLog(PrestoFileSystemCache.class);

    private final AtomicLong unique = new AtomicLong();
    private final Map<FileSystemKey, FileSystemHolder> map = new HashMap<>();

    @Override
    FileSystem get(URI uri, Configuration conf)
            throws IOException
    {
        return getInternal(uri, conf, 0);
    }

    @Override
    FileSystem getUnique(URI uri, Configuration conf)
            throws IOException
    {
        return getInternal(uri, conf, unique.incrementAndGet());
    }

    private synchronized FileSystem getInternal(URI uri, Configuration conf, long unique)
            throws IOException
    {
        UserGroupInformation userGroupInformation = UserGroupInformation.getCurrentUser();
        FileSystemKey key = createFileSystemKey(uri, userGroupInformation, unique);
        Set<?> privateCredentials = getPrivateCredentials(userGroupInformation);

        FileSystemHolder fileSystemHolder = map.get(key);
        if (fileSystemHolder == null) {
            int maxSize = conf.getInt("fs.cache.max-size", 1000);
            if (map.size() >= maxSize) {
                throw new IOException(format("FileSystem max cache size has been reached: %s", maxSize));
            }
            FileSystem fileSystem = createFileSystem(uri, conf);
            fileSystemHolder = new FileSystemHolder(fileSystem, privateCredentials);
            map.put(key, fileSystemHolder);
        }

        // Private credentials are only set when using Kerberos authentication.
        // When the user is the same, but the private credentials are different,
        // that means that Kerberos ticket has expired and re-login happened.
        // To prevent cache leak in such situation, the privateCredentials are not
        // a part of the FileSystemKey, but part of the FileSystemHolder. When a
        // Kerberos re-login occurs, re-create the file system and cache it using
        // the same key.
        if (isHdfs(uri) && !fileSystemHolder.getPrivateCredentials().equals(privateCredentials)) {
            map.remove(key);
            FileSystem fileSystem = createFileSystem(uri, conf);
            fileSystemHolder = new FileSystemHolder(fileSystem, privateCredentials);
            map.put(key, fileSystemHolder);
        }

        return fileSystemHolder.getFileSystem();
    }

    private static FileSystem createFileSystem(URI uri, Configuration conf)
            throws IOException
    {
        Class<?> clazz = getFileSystemClass(uri.getScheme(), conf);
        if (clazz == null) {
            throw new IOException("No FileSystem for scheme: " + uri.getScheme());
        }
        final FileSystem original = (FileSystem) ReflectionUtils.newInstance(clazz, conf);
        original.initialize(uri, conf);
        FilterFileSystem wrapper = new FileSystemWrapper(original);
        FinalizerService.getInstance().addFinalizer(wrapper, new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    original.close();
                }
                catch (IOException e) {
                    log.error("Error occurred when finalizing file system", e);
                }
            }
        });
        return wrapper;
    }

    @Override
    synchronized void remove(Key ignored, FileSystem fileSystem)
    {
        if (fileSystem == null) {
            return;
        }
        Iterator<Entry<FileSystemKey, FileSystemHolder>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            if (fileSystem.equals(iterator.next().getValue().getFileSystem())) {
                iterator.remove();
                break;
            }
        }
    }

    @Override
    synchronized void closeAll()
            throws IOException
    {
        for (FileSystemHolder fileSystemHolder : map.values()) {
            fileSystemHolder.getFileSystem().close();
        }
        map.clear();
    }

    @Override
    synchronized void closeAll(boolean onlyAutomatic)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    synchronized void closeAll(UserGroupInformation ugi)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    private static FileSystemKey createFileSystemKey(URI uri, UserGroupInformation userGroupInformation, long unique)
    {
        String scheme = nullToEmpty(uri.getScheme()).toLowerCase(ENGLISH);
        String authority = nullToEmpty(uri.getAuthority()).toLowerCase(ENGLISH);
        String realUser;
        String proxyUser;
        AuthenticationMethod authenticationMethod = userGroupInformation.getAuthenticationMethod();
        switch (authenticationMethod) {
            case SIMPLE:
            case KERBEROS:
                realUser = userGroupInformation.getUserName();
                proxyUser = null;
                break;
            case PROXY:
                realUser = userGroupInformation.getRealUser().getUserName();
                proxyUser = userGroupInformation.getUserName();
                break;
            default:
                throw new IllegalArgumentException("Unsupported authentication method: " + authenticationMethod);
        }
        return new FileSystemKey(scheme, authority, unique, realUser, proxyUser);
    }

    private static Set<?> getPrivateCredentials(UserGroupInformation userGroupInformation)
    {
        AuthenticationMethod authenticationMethod = userGroupInformation.getAuthenticationMethod();
        switch (authenticationMethod) {
            case SIMPLE:
                return ImmutableSet.of();
            case KERBEROS:
                return ImmutableSet.copyOf(getSubject(userGroupInformation).getPrivateCredentials());
            case PROXY:
                return getPrivateCredentials(userGroupInformation.getRealUser());
            default:
                throw new IllegalArgumentException("Unsupported authentication method: " + authenticationMethod);
        }
    }

    private static boolean isHdfs(URI uri)
    {
        String scheme = uri.getScheme();
        return "hdfs".equals(scheme) || "viewfs".equals(scheme);
    }

    private static class FileSystemKey
    {
        private final String scheme;
        private final String authority;
        private final long unique;
        private final String realUser;
        private final String proxyUser;

        public FileSystemKey(String scheme, String authority, long unique, String realUser, String proxyUser)
        {
            this.scheme = requireNonNull(scheme, "scheme is null");
            this.authority = requireNonNull(authority, "authority is null");
            this.unique = unique;
            this.realUser = requireNonNull(realUser, "realUser");
            this.proxyUser = proxyUser;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileSystemKey that = (FileSystemKey) o;
            return Objects.equals(scheme, that.scheme) &&
                    Objects.equals(authority, that.authority) &&
                    Objects.equals(unique, that.unique) &&
                    Objects.equals(realUser, that.realUser) &&
                    Objects.equals(proxyUser, that.proxyUser);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(scheme, authority, unique, realUser, proxyUser);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("scheme", scheme)
                    .add("authority", authority)
                    .add("unique", unique)
                    .add("realUser", realUser)
                    .add("proxyUser", proxyUser)
                    .toString();
        }
    }

    private static class FileSystemHolder
    {
        private final FileSystem fileSystem;
        private final Set<?> privateCredentials;

        public FileSystemHolder(FileSystem fileSystem, Set<?> privateCredentials)
        {
            this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
            this.privateCredentials = ImmutableSet.copyOf(requireNonNull(privateCredentials, "privateCredentials is null"));
        }

        public FileSystem getFileSystem()
        {
            return fileSystem;
        }

        public Set<?> getPrivateCredentials()
        {
            return privateCredentials;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("fileSystem", fileSystem)
                    .add("privateCredentials", privateCredentials)
                    .toString();
        }
    }

    private static class FileSystemWrapper
            extends FilterFileSystem
    {
        public FileSystemWrapper(FileSystem fs)
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

    private static class OutputStreamWrapper
            extends FSDataOutputStream
    {
        private final FileSystem fileSystem;

        public OutputStreamWrapper(FSDataOutputStream delegate, FileSystem fileSystem)
                throws IOException
        {
            super(delegate, null, delegate.getPos());
            this.fileSystem = fileSystem;
        }
    }

    private static class InputStreamWrapper
            extends FSDataInputStream
    {
        private final FileSystem fileSystem;

        public InputStreamWrapper(FSDataInputStream inputStream, FileSystem fileSystem)
        {
            super(inputStream);
            this.fileSystem = fileSystem;
        }
    }
}
