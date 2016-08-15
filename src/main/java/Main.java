import com.google.api.client.http.GenericUrl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.Protos.FrameworkID;
import org.apache.mesos.v1.scheduler.Protos.Event;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = Logger.getLogger(Main.class.getName());
        ExecutorService threadPool = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setDaemon(true).build());

        FrameworkID id = FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build();
        FrameworkInfo info = FrameworkInfo.newBuilder().setUser("user").setName("name").setId(id).build();

        // REPLACE THIS
        String url = "http://192.168.33.7:5050";

        SchedulerClient c = new SchedulerClient(new GenericUrl(url + "/api/v1/scheduler"));

        Stream<Event> stream = c.subscribe(info);

        // consume the event stream
        threadPool.submit(() -> stream.forEach(event -> logger.info(event.toString())));

        // Simulate some wait before sending the teardown request
        Thread.sleep(1000);

        c.teardown();

        // Block for a bit before exiting
        threadPool.awaitTermination(20, TimeUnit.SECONDS);
    }
}
