import com.azure.core.credential.AzureKeyCredential;
import com.azure.search.documents.SearchAsyncClient;
import com.azure.search.documents.SearchClientBuilder;
import com.azure.search.documents.models.*;

import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

public class UniqueAccountDetailsService {

    private final SearchAsyncClient searchClient;

    public UniqueAccountDetailsService(String endpoint, String indexName, String apiKey) {
        this.searchClient = new SearchClientBuilder()
                .credential(new AzureKeyCredential(apiKey))
                .endpoint(endpoint)
                .indexName(indexName)
                .buildAsyncClient();
    }

    public Mono<Map<String, Integer>> fetchUniqueAccountDetailsCountById() {
        SearchOptions options = new SearchOptions()
                .setAggregations("id,countdistinct(accountDetails)")
                .setTop(0);

        return searchClient.search("*", options)
                .byPage()
                .next()
                .map(page -> {
                    Map<String, Integer> result = new HashMap<>();
                    if (page.getAggregations() != null && page.getAggregations().containsKey("id")) {
                        page.getAggregations().get("id").forEach(r -> {
                            String id = r.getValue().toString();
                            Integer count = r.getCount(); // distinct accountDetails count
                            result.put(id, count);
                        });
                    }
                    return result;
                });
    }
}
