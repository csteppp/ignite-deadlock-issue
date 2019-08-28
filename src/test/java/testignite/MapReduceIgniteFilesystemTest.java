package testignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskMapAsync;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.mapreduce.IgfsFileRange;
import org.apache.ignite.igfs.mapreduce.IgfsJob;
import org.apache.ignite.igfs.mapreduce.IgfsTask;
import org.apache.ignite.igfs.mapreduce.IgfsTaskArgs;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;


/**
 * This test demonstrates two problems in Ignite Filesystem.
 * 1.  Why do map reduce jobs run in the SYS pool instead oWf the PUBLIC pool?
 * 2.  Any map reduce job that writes or reads from igfs will cause a deadlock .
 *
 * Help!
 */
public class MapReduceIgniteFilesystemTest {

    private Ignite serverIgnite;

    private Ignite clientIgnite;
    private IgniteFileSystem igfsClient;

    @Before
    public void startupConnectIgnite() throws Exception{

        FileSystemConfiguration fileSystemConfiguration = new FileSystemConfiguration()
                .setName("test");

        // start up the server
        IgniteConfiguration igniteServerConfig = new IgniteConfiguration()
                .setIgniteInstanceName("server")
                .setActiveOnStart(true)
                .setAutoActivationEnabled(true)
                .setAuthenticationEnabled(false)
                .setClientMode(false)
                .setFileSystemConfiguration(fileSystemConfiguration)
                // we are doing two here to make it small to show the deadlock problem.
                .setSystemThreadPoolSize(2);
        serverIgnite = IgnitionEx.start(igniteServerConfig);

        // connect the client
        clientIgnite = IgnitionEx.start(
                new IgniteConfiguration()
                        .setIgniteInstanceName("client")
                        .setActiveOnStart(true)
                        .setAutoActivationEnabled(true)
                        .setAuthenticationEnabled(false)
                        .setClientMode(true)
                        .setFileSystemConfiguration(fileSystemConfiguration)
                        .setSystemThreadPoolSize(2)
        );
        igfsClient = clientIgnite.fileSystem("test");
    }

    @After
    public void shutdown(){
        clientIgnite.close();
        serverIgnite.close();
    }


    @Test
    public void test() throws IOException {

        final IgfsPath affinityPath = new IgfsPath("/test/affinity.dat");
        try(IgfsOutputStream out = igfsClient.create(affinityPath, true)){
            out.write("I am!".getBytes(Charset.defaultCharset()));
        }

        final IgfsPath path = new IgfsPath("/test/file1.dat");

        // create a file, and write to it something arbitrary.  Do it three times to fill up the threadpool and
        // show the deadlock.
        Collection<IgniteFuture<Object>> myFutures = new ArrayList<>();
        for(int i = 0; i < 2; i++){
            myFutures.add(igfsClient.executeAsync(new IgfsTaskThatWrites(path, igfsClient),
                    null,
                    Collections.singleton(affinityPath),
                    null
            ));
        }

        for(IgniteFuture<Object> future: myFutures)
            future.get(10, TimeUnit.SECONDS);

    }

    /**
     * A class that deadlocks because
     */
    @ComputeTaskMapAsync
    private static class IgfsTaskThatWrites extends IgfsTask<Object, Object> {
        private final IgfsPath path;
        private final IgniteFileSystem igfsClient;

        public IgfsTaskThatWrites(IgfsPath path, IgniteFileSystem igfsClient) {
            this.path = path;
            this.igfsClient = igfsClient;
        }

        @Nullable
        @Override
        public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            try {
                // show using the threadname that it runs in the SYS thread pool which is weird.  Shouldn't it be in public thread pool?
                System.out.println("reduce in " + Thread.currentThread().getName());
                assertTrue(Thread.currentThread().getName().startsWith("sys"));


                // sleeping to give this first task time to start up and wait for the second tasks so we fill up the threadpool
                // before doing our write
                Thread.sleep(2000);

                // create a output stream and write to it.  This uses ANOTHER thread from the ignite
                // system pool.  that means that this thread depends on another thread in the same pool,
                // which when the pool is full causes threadpool starvation and an actual deadlock.
                try(IgfsOutputStream out = igfsClient.create(path, true)) {
                    out.write("The hills are alive with the sound of music!".getBytes(Charset.defaultCharset()));
                }

                return path;
            } catch (Exception e) {
                e.printStackTrace();
              throw new IgniteException(e);
            }
        }

        @Nullable
        @Override
        public IgfsJob createJob(IgfsPath path, IgfsFileRange range, IgfsTaskArgs<Object> args) throws IgniteException {
            return new IgfsJob() {
                @Override
                public Object execute(IgniteFileSystem igfs, IgfsFileRange range, IgfsInputStream in) throws IgniteException, IOException {
                    System.out.println("execute in " + Thread.currentThread().getName());
                    try {
                        Thread.sleep(100);
                        return new Object();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        throw new IgniteException(e);
                    }
                }

                @Override
                public void cancel() {
                    System.out.println("Cancel in " + Thread.currentThread().getName());
                }
            };
        }
    }
}
