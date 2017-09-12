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

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class FinalizerService
{
    public static final Log log = LogFactory.getLog(FinalizerService.class);

    private static final long REMOVE_TIMEOUT = 5000;

    private static FinalizerService INSTANCE;

    private final Set<FinalizerReference> finalizers = Sets.newSetFromMap(new ConcurrentHashMap<FinalizerReference, Boolean>());
    private final ReferenceQueue<Object> finalizerQueue = new ReferenceQueue<>();
    private Thread finalizerThread;

    private FinalizerService() {}

    public static synchronized FinalizerService getInstance()
    {
        if (INSTANCE == null) {
            FinalizerService finalizer = new FinalizerService();
            finalizer.start();
            INSTANCE = finalizer;
        }
        return INSTANCE;
    }

    private void start()
    {
        if (finalizerThread != null) {
            return;
        }
        finalizerThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                processFinalizerQueue();
            }
        });
        finalizerThread.setDaemon(true);
        finalizerThread.setName("hadoop-finalizer-service");
        finalizerThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            @Override
            public void uncaughtException(Thread t, Throwable e)
            {
                log.error("Uncaught exception in finalizer thread", e);
            }
        });
        finalizerThread.start();
    }

    /**
     * When referent is freed by the garbage collector, run cleanup.
     * <p>
     * Note: cleanup must not contain a reference to the referent object.
     */
    public void addFinalizer(Object referent, Runnable cleanup)
    {
        requireNonNull(referent, "referent is null");
        requireNonNull(cleanup, "cleanup is null");
        finalizers.add(new FinalizerReference(referent, finalizerQueue, cleanup));
    }

    private void processFinalizerQueue()
    {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                FinalizerReference finalizer = (FinalizerReference) finalizerQueue.remove(REMOVE_TIMEOUT);
                if (finalizer != null) {
                    finalizers.remove(finalizer);
                    finalizer.cleanup();
                }
            }
            catch (InterruptedException e) {
                return;
            }
            catch (Throwable e) {
                log.error("Exception in finalizer queue processor", e);
            }
        }
    }

    private static class FinalizerReference
            extends PhantomReference<Object>
    {
        private final Runnable cleanup;
        private final AtomicBoolean executed = new AtomicBoolean();

        public FinalizerReference(Object referent, ReferenceQueue<Object> queue, Runnable cleanup)
        {
            super(requireNonNull(referent, "referent is null"), requireNonNull(queue, "queue is null"));
            this.cleanup = requireNonNull(cleanup, "cleanup is null");
        }

        public void cleanup()
        {
            if (executed.compareAndSet(false, true)) {
                cleanup.run();
            }
        }
    }
}
