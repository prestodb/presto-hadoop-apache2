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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 *  Manage FileSystemHolders that removed from PrestoFileSystemCache, and close them after PRESTO_HDFS_EXPIRED_FS_DELAY_CLOSE_TIME.
 *  To avoid Filesystem closed Exception caused by closing FileSystem in use.
 */
public class FileSystemCleaner
{
    public static final Log log = LogFactory.getLog(FileSystemCleaner.class);
    public static final String PRESTO_HDFS_EXPIRED_FS_DELAY_CLOSE_TIME = "presto.hdfs.expired.fs.delay.close.time";
    public static final String PRESTO_HDFS_EXPIRED_FS_CHECK_INTERVAL = "presto.hdfs.expired.fs.check.interval";
    public static final long DEFAULT_PRESTO_HDFS_FS_CACHE_DELAY_CLOSE_TIME = 5 * 60 * 1000; // five minutes
    public static final long DEFAULT_PRESTO_HDFS_EXPIRED_FS_CHECK_INTERVAL = 60 * 1000;  // one minute

    private static final FileSystemCleaner manager = new FileSystemCleaner();
    private static final List<PrestoFileSystemCache.FileSystemHolder> fileSystemHolderList = new LinkedList<>();

    public FileSystemCleaner()
    {
        Thread fileSystemCleanerTask = new FileSystemCleanerTask();
        fileSystemCleanerTask.setDaemon(true);
        fileSystemCleanerTask.setName("FileSystemCleanerTask");
        fileSystemCleanerTask.start();
    }

    public static FileSystemCleaner getInstance()
    {
        return manager;
    }

    public void addExpiredFileSystem(PrestoFileSystemCache.FileSystemHolder fileSystemHolder)
    {
        fileSystemHolder.setExpireTimestamp(System.currentTimeMillis());
        fileSystemHolderList.add(fileSystemHolder);
    }

    private static class FileSystemCleanerTask
            extends Thread
    {
        @Override
        public void run()
        {
            while (true) {
                for (PrestoFileSystemCache.FileSystemHolder holder : fileSystemHolderList) {
                    long delayCloseTime = holder.getFileSystem().getConf().getLong(PRESTO_HDFS_EXPIRED_FS_DELAY_CLOSE_TIME,
                            DEFAULT_PRESTO_HDFS_FS_CACHE_DELAY_CLOSE_TIME);
                    if (System.currentTimeMillis() - holder.getExpireTimestamp() >= delayCloseTime) {
                        try {
                            log.info(String.format("Closing expired FileSystem{expireTimestamp: %s, delayCloseTime: %s ms}",
                                    holder.getExpireTimestamp(), delayCloseTime));
                            holder.getFileSystem().close();
                            fileSystemHolderList.remove(holder);
                        }
                        catch (IOException e) {
                            log.error("Close expired file system fail ", e);
                        }
                    }
                    else {
                        break;
                    }
                }

                try {
                    Thread.sleep(DEFAULT_PRESTO_HDFS_EXPIRED_FS_CHECK_INTERVAL);
                }
                catch (InterruptedException e) {
                    log.error(e);
                }
            }
        }
    }
}
