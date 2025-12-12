package org.moto.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.moto.models.DriverDocument;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class LambdaHandler implements RequestHandler<APIGatewayProxyRequestEvent , APIGatewayProxyResponseEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();
    public static final String TABLE_NAME = "Drivers";
    public static final Region REGION = Region.US_EAST_1;
    public static final DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(REGION).build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent reqBody, Context context) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        String body = reqBody.getBody();
        String jsonBody;
        if (reqBody.getIsBase64Encoded() != null && reqBody.getIsBase64Encoded()) {
            byte[] decoded = Base64.getDecoder().decode(body);
            jsonBody = new String(decoded, StandardCharsets.UTF_8);
        } else {
            jsonBody = body;
        }
        try {
            DriverDocument driverDocument = mapper.readValue(jsonBody, DriverDocument.class);

            Map<String, AttributeValue> key = new HashMap<>();
            key.put("driverID", AttributeValue.builder().s(driverDocument.getDriverID()).build());

            AttributeValue newPos = AttributeValue.builder().m(Map.of(
                            "latitude", AttributeValue.builder().n(driverDocument.getPositions().get(0).getLatitude().toString()).build(),
                            "longitude", AttributeValue.builder().n(driverDocument.getPositions().get(0).getLongitude().toString()).build()
                    ))
                    .build();
            AttributeValue emptyList = AttributeValue.builder().m(Collections.emptyMap()).build();
            Map<String, AttributeValue> eav = new HashMap<>();
            eav.put(":newPos", AttributeValue.builder().l(newPos).build());

            UpdateItemRequest req = UpdateItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(key)
                    .updateExpression("SET positions = list_append(if_not_exists(positions, :emptyList), :newPos)")
                    .expressionAttributeValues(Map.of(
                            ":emptyList", AttributeValue.builder().l(emptyList).build(),
                            ":newPos", AttributeValue.builder().l(newPos).build()
                    ))
                    .build();

            dynamoDbClient.updateItem(req);

            context.getLogger().log(String.format("Driver position updated with ID: %s", driverDocument.getDriverID()));
        } catch (JsonProcessingException e) {
            context.getLogger().log(String.format("JSON exception while processing request: %s", e.getMessage()));
            response.setStatusCode(500);
            return response;
        }
        response.setStatusCode(200);
        response.setBody("Position updated");
        return response;
    }
}
