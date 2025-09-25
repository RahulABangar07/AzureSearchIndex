import com.azure.core.credential.AzureKeyCredential;
import com.azure.search.documents.SearchAsyncClient;
import com.azure.search.documents.SearchClientBuilder;
import com.azure.search.documents.models.*;

import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DistinctIdFetcher {

    private final SearchAsyncClient searchClient;

    public DistinctIdFetcher(String endpoint, String indexName, String apiKey) {
        this.searchClient = new SearchClientBuilder()
                .credential(new AzureKeyCredential(apiKey))
                .endpoint(endpoint)
                .indexName(indexName)
                .buildAsyncClient();
    }

    public Mono<Set<String>> fetchAllDistinctIds() {
        SearchOptions options = new SearchOptions()
                .setFacets("id,count:1000") // facet on id array field
                .setTop(0); // don’t fetch docs, only facets

        return searchClient.search("*", options)
                .byPage()
                .next()
                .map(page -> {
                    Set<String> distinctIds = new HashSet<>();
                    Map<String, List<FacetResult>> facets = page.getFacets();
                    if (facets != null && facets.containsKey("id")) {
                        facets.get("id").forEach(facet ->
                                distinctIds.add(facet.getValue().toString()));
                    }
                    return distinctIds;
                });
    }

    public static void main(String[] args) {
        DistinctIdFetcher fetcher =
                new DistinctIdFetcher("<YOUR-ENDPOINT>", "<YOUR-INDEX-NAME>", "<YOUR-API-KEY>");

        fetcher.fetchAllDistinctIds().subscribe(ids -> {
            System.out.println("Distinct IDs:");
            ids.forEach(System.out::println);
        });
    }
}
