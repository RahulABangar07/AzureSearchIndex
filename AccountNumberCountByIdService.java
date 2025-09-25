import com.azure.core.credential.AzureKeyCredential;
import com.azure.search.documents.SearchAsyncClient;
import com.azure.search.documents.SearchClientBuilder;
import com.azure.search.documents.models.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

public class AccountNumberCountByIdService {

    private final SearchAsyncClient searchClient;

    public AccountNumberCountByIdService(String endpoint, String indexName, String apiKey) {
        this.searchClient = new SearchClientBuilder()
                .credential(new AzureKeyCredential(apiKey))
                .endpoint(endpoint)
                .indexName(indexName)
                .buildAsyncClient();
    }

    public Mono<Map<String, Integer>> fetchAccountNumberCountById() {
        // Step 1: fetch distinct ids
        SearchOptions idOptions = new SearchOptions()
                .setFacets("id,count:1000")
                .setTop(0);

        return searchClient.search("*", idOptions)
                .byPage()
                .next()
                .flatMapMany(page -> Flux.fromIterable(page.getFacets().get("id")))
                .flatMap(idFacet -> {
                    String idValue = idFacet.getValue().toString();

                    // Step 2: facet accountNumber for this id
                    SearchOptions accOptions = new SearchOptions()
                            .setFilter("id/any(x: x eq '" + idValue + "')") // array filter
                            .setFacets("accountNumber,count:1000")
                            .setTop(0);

                    return searchClient.search("*", accOptions)
                            .byPage()
                            .next()
                            .map(accPage -> {
                                List<FacetResult> accFacets = accPage.getFacets().get("accountNumber");
                                int uniqueCount = accFacets != null ? accFacets.size() : 0;
                                return Map.entry(idValue, uniqueCount);
                            });
                })
                .collectMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    public static void main(String[] args) {
        AccountNumberCountByIdService service =
                new AccountNumberCountByIdService("<YOUR-ENDPOINT>", "<YOUR-INDEX-NAME>", "<YOUR-API-KEY>");

        service.fetchAccountNumberCountById()
                .subscribe(result -> result.forEach((id, count) ->
                        System.out.println("id=" + id + ", uniqueAccountNumberCount=" + count)
                ));
    }
}
