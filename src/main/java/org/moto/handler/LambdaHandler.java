package org.moto.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.moto.models.Position;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class LambdaHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();
    public static final String TABLE_NAME = "DriversPositions";
    public static final Region REGION = Region.US_EAST_1;
    public static final DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(REGION).build();
    public static final DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(dynamoDbClient).build();
    public static final DynamoDbTable<Position> positionTable = enhancedClient.table(TABLE_NAME,
            TableSchema.fromBean(Position.class));

    /**
     * Handles the position request from API Gateway
     * 
     * @param reqBody The request body that contains the position data
     * @param context The context of the request
     * @return The response from the request
     */
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
            Position position = mapper.readValue(jsonBody, Position.class);
            positionTable.putItem(position);
            context.getLogger().log(String.format("DriverID: %s position updated with timestamp: %s",
                    position.getDriverID(), position.getTimestamp()));
        } catch (JsonProcessingException e) {
            context.getLogger().log(String.format("JSON exception while processing request: %s", e.getMessage()));
            response.setStatusCode(500);
            return response;
        }
        response.setStatusCode(200);
        response.setBody("Position updated successfully");
        return response;
    }
}
