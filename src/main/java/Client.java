import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

public class Client {
    private static final String ENDPOINT = "https://sample-538240208154493003.int.myfreshworks.dev/crm/sales/";
    private static final String UPSERT = "api/contacts/upsert";
    private static final String TOKEN = "XuLQgulEjrQ0OZI2foufqQ";
    private static final AtomicInteger i = new AtomicInteger();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        List<Future<?>> tasks = new ArrayList<>();
        for(int i=0;i<100;i++) {
            Future<?> task = executor.submit(Client::fire);
            tasks.add(task);
        }

        for (Future<?> task : tasks) {
            task.get();
        }
        executor.shutdown();
    }

    public static void fire() {
        OkHttpClient client = new OkHttpClient();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, buildPayload(i.incrementAndGet()));
        Request request = new Request.Builder()
            .url(ENDPOINT + UPSERT)
            .method("POST", body)
            .addHeader("Authorization", "Token token="+TOKEN)
            .addHeader("Content-Type", "application/json")
            .build();

        try {
            Response response = client.newCall(request).execute();
            if(response.code()==200)
                System.out.println("Response code:: " + response.code());
            else
                System.out.println("Response code:: "+ response.code() + "message: "+ response.body().toString());
        } catch (IOException e){
            System.out.println("Error:: "+e.getMessage());
        }
    }

    private static String buildPayload(int index) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.set("unique_identifier", identifer());
            objectNode.set("contact", contact(index));
            return mapper.writeValueAsString(objectNode);
        } catch (JsonProcessingException jpe){
            jpe.printStackTrace();
        }
        return "";
    }

    private static ObjectNode identifer() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put("emails", "test1@yopmail.com");
        return objectNode;
    }

    private static ObjectNode contact(int index) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put("first_name", "user_first_name_"+index);
        objectNode.put("last_name", "user_last_name_"+index);
        return objectNode;
    }
}
