import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;

import java.io.File;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
public class OrientDbJavaExample {
    public static void main(String[] args) throws Exception {

        final OrientGraphFactory factory = new OrientGraphFactory("remote:localhost/test00")
                .setLabelAsClassName(true)
                .setLightWeightEdge(true)
                .setupPool(4, -1);

        Queue<String> queue = new ConcurrentLinkedQueue<String>();
        queue.addAll(Files.readLines(new File("spouse"), Charsets.UTF_8));

        ExecutorService threadPool = Executors.newSingleThreadExecutor();

        List<Future<Boolean>> futures = Lists.newArrayList();
        for (int i=0; i<4; i++){
            Future<Boolean> future = threadPool.submit(() -> {
                while (!queue.isEmpty()){
                    String q = queue.poll();
                    long t0 = System.nanoTime();
                    OrientGraph g = factory.getNoTx();
                    long t1 = System.nanoTime();
                    List<Object> list = g.traversal().V().has("Person", "name", q).out("isActorFor").in
                            ("hasActor")
                            .values("title").toList();
                    long t2 = System.nanoTime();
                    g.close();
                    long t3 = System.nanoTime();
                    if(t3 - t0 > 100 * 1000000){
                        System.err.println(
                                list.size() + " cost: " + (t1 - t0) / 1000000 + ", " + (t2 - t1) / 1000000
                                        + ", " + (t3 - t2) / 1000000 + " " + q);
                    }
                }
                return true;
            });
            futures.add(future);
        }
        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });

    }
}
