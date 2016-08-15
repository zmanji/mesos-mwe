import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.primitives.Bytes;
import org.apache.mesos.v1.Protos.FrameworkInfo;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.apache.mesos.v1.scheduler.Protos.Call;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

class SchedulerClient {

  private Event readMessage(InputStream is) throws IOException {
    // Read header
    List<Byte> buf = new ArrayList<>();
    int b;
    while ((b = is.read()) != '\n') {
      if (b == -1) {
        throw new EOFException();
      }
      buf.add((byte) b);
    }

    Long l = Long.parseLong(new String(Bytes.toArray(buf), StandardCharsets.UTF_8));

    buf = new ArrayList<>();

    for (long i = 0; i < l; i++) {
      b = is.read();
      if (b == -1) {
        throw new EOFException();
      }
      buf.add((byte) b);
    }

    return Event.parseFrom(Bytes.toArray(buf));
  }

  private HttpRequest makeHttpRequest(Call c) throws IOException {
    String contentType = "application/x-protobuf";
    HttpContent content = new ByteArrayContent(contentType, c.toByteArray());
    NetHttpTransport transport = new NetHttpTransport();
    HttpRequestFactory requestFactory = transport.createRequestFactory();
    HttpRequest r = requestFactory.buildPostRequest(url, content);
    HttpHeaders headers = r.getHeaders();
    headers.setAccept(contentType);

    return r;
  }

  private GenericUrl url;
  private LinkedBlockingQueue<Event> eventBuf = new LinkedBlockingQueue<>();
  private ExecutorService executor = Executors.newFixedThreadPool(4);
  private String streamId;
  private FrameworkInfo frameworkInfo;
  private Logger logger = Logger.getLogger(SchedulerClient.class.getName());

  SchedulerClient(GenericUrl url) {
    this.url = requireNonNull(url);
  }

  Stream<Event> subscribe(FrameworkInfo info) throws IOException {
    frameworkInfo = info;

    Call c = Call.newBuilder().setType(Call.Type.SUBSCRIBE)
        .setSubscribe(Call.Subscribe.newBuilder().setFrameworkInfo(info)).setFrameworkId(info.getId()).build();
    HttpRequest r = makeHttpRequest(c);

    logger.info("Request Headers: " + r.getHeaders());
    logger.info("Request Body " + c);

    HttpResponse response = r.execute();

    logger.info("Response Headers: " + response.getHeaders());
    streamId = response.getHeaders().get("Mesos-Stream-Id").toString();

    executor.execute(() -> {
      try {
        while(true) {
          Event e = readMessage(response.getContent());
          eventBuf.add(e);
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    });

    return Stream.generate(() -> {
      try {
        return eventBuf.take();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    });
  }

  void teardown() throws IOException {
    requireNonNull(streamId);

    Call c = Call.newBuilder().setType(Call.Type.TEARDOWN).setFrameworkId(frameworkInfo.getId()).build();
    HttpRequest r = makeHttpRequest(c);
    r.getHeaders().set("Mesos-Stream-Id", streamId);

    logger.info("Request Headers: " + r.getHeaders());
    logger.info("Request Body: " + c);

    r.execute();
  }
}
