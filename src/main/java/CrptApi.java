import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.util.concurrent.RateLimiter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class CrptApi {
    private static final String CREATE_REQUEST_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create?pg=%s";
    private final RateLimiter rateLimiter;
    private final HttpClient httpClient;
    private final Supplier<String> authTokenSupplier;

    public CrptApi(TimeUnit timeUnit, int requestLimit, Supplier<String> authTokenSupplier) {
        this.rateLimiter = RateLimiter.create(permitsPerSecond(timeUnit, requestLimit));
        this.httpClient = HttpClient.newHttpClient();
        this.authTokenSupplier = authTokenSupplier;
    }

    public HttpResponse<String> createDocument(Document document, String signature, String productGroup) throws IOException, InterruptedException {
        rateLimiter.acquire();

        CreateDocumentRequest requestBody = CreateDocumentRequest.builder()
                .document_format(DocumentFormat.MANUAL)
                .product_document(encodeToBase64(document))
                .product_group(productGroup)
                .signature(signature)
                .type(Type.LP_INTRODUCE_GOODS)
                .build();
        String jsonRequestBody = ParsingUtil.writeObjectAsJsonString(requestBody);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format(CREATE_REQUEST_URL, productGroup)))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + authTokenSupplier.get())
                .POST(HttpRequest.BodyPublishers.ofString(jsonRequestBody))
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private double permitsPerSecond(TimeUnit timeUnit, int requestLimit) {
        long seconds = timeUnit.toSeconds(1);
        return (double) requestLimit / seconds;
    }

    private String encodeToBase64(Document document) {
        String json = ParsingUtil.writeObjectAsJsonString(document);
        return Base64.getEncoder().encodeToString(json.getBytes());
    }

    enum DocumentFormat {
        MANUAL, XML, CSV;
    }

    enum Type {
        LP_INTRODUCE_GOODS,
        LP_INTRODUCE_GOODS_CSV,
        LP_INTRODUCE_GOODS_XML
    }

    @Getter
    @Builder
    @Jacksonized
    static class CreateDocumentRequest {
        private final DocumentFormat document_format;
        private final String product_document;
        private final String product_group;
        private final String signature;
        private final Type type;
    }

    @Jacksonized
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    static class Document {
        private Description description;
        private String doc_id;
        private String doc_status;
        private String doc_type;
        private boolean importRequest;
        private String owner_inn;
        private String participant_inn;
        private String producer_inn;
        private String production_date;
        private String production_type;
        private Product[] products;
        private String reg_date;
        private String reg_number;
    }

    @Jacksonized
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    static class Product {
        private String certificate_document;
        private String certificate_document_date;
        private String certificate_document_number;
        private String owner_inn;
        private String producer_inn;
        private String production_date;
        private String tnved_code;
        private String uit_code;
        private String uitu_code;
    }

    @Jacksonized
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    static class Description {
        private String participantInn;
    }

    private static class ParsingUtil {
        private static final ObjectMapper defaultMapper = new ObjectMapper()
                .configure(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS, true)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .enable(SerializationFeature.INDENT_OUTPUT);

        static <T> String writeObjectAsJsonString(T object) {
            return writeObjectAsJsonString(object, defaultMapper);
        }

        static <T> String writeObjectAsJsonString(T object, ObjectMapper mapper) {
            try {
                return mapper.writeValueAsString(object);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Возникла ошибка при конвертации объекта в строку" , e);
            }
        }
    }
}