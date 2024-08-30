package com.github.ansd;

import java.util.HashMap;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import static com.rabbitmq.client.amqp.Management.ExchangeType.HEADERS;
import static com.rabbitmq.client.amqp.Management.ExchangeType.FANOUT;
import static com.rabbitmq.client.amqp.Management.QuorumQueueDeadLetterStrategy.AT_LEAST_ONCE;
import static com.rabbitmq.client.amqp.Management.OverflowStrategy.REJECT_PUBLISH;

public class App {
    public static void main(String[] args) {

        Environment environment = new AmqpEnvironmentBuilder().build();
        Connection connection = environment.connectionBuilder().build();
        Management management = connection.management();

        String headersExchange = "my-headers-exchange";
        String alternateExchange = "my-alternate-exchange";
        String ordersQueue = "orders";
        String transientFailuresDLQ = "transient-failures-dlq";
        String businessLogicFailuresDLQ = "business-logic-failures-dlq";
        String uncategorisedDLQ = "uncategorised-dlq";

        management
            .exchange()
            .name(headersExchange)
            .type(HEADERS)
            .argument("alternate-exchange", alternateExchange)
            .declare();

        management
            .exchange()
            .name(alternateExchange)
            .type(FANOUT)
            .declare();

        management
            .queue()
            .name(ordersQueue)
            .overflowStrategy(REJECT_PUBLISH)
            .deadLetterExchange(headersExchange)
            .quorum()
            .deadLetterStrategy(AT_LEAST_ONCE)
            .queue()
            .declare();

        management
            .queue()
            .name(transientFailuresDLQ)
            .quorum()
            .queue()
            .declare();

        Map<String, Object> transientFailuresBindingArgs = new HashMap<>();
        transientFailuresBindingArgs.put("x-match", "all-with-x");
        transientFailuresBindingArgs.put("x-opt-dead-letter-category", "transient");
        management.binding()
            .sourceExchange(headersExchange)
            .destinationQueue(transientFailuresDLQ)
            .arguments(transientFailuresBindingArgs)
            .bind();

        management
            .queue()
            .name(businessLogicFailuresDLQ)
            .quorum()
            .queue()
            .declare();

        Map<String, Object> businessFailuresBindingArgs = new HashMap<>();
        businessFailuresBindingArgs.put("x-match", "all-with-x");
        businessFailuresBindingArgs.put("x-opt-dead-letter-category", "business-logic");
        management.binding()
            .sourceExchange(headersExchange)
            .destinationQueue(businessLogicFailuresDLQ)
            .arguments(businessFailuresBindingArgs)
            .bind();

        // Every dead lettered message whitout a matching category will be sent to this stream.
        management
            .queue()
            .name(uncategorisedDLQ)
            .stream()
            .queue()
            .declare();

        management.binding()
            .sourceExchange(alternateExchange)
            .destinationQueue(uncategorisedDLQ)
            .bind();

        // Publish a message to the orders queue.
        Publisher publisher = connection.publisherBuilder()
            .queue(ordersQueue)
            .build();
        Message msg = publisher.message("order 1".getBytes(StandardCharsets.UTF_8));
        publisher.publish(msg, context -> {
            System.out.printf("publisher: received %s outcome\n", context.status());
        });

        // Consume message from the orders queue and dead letter it with a custom reason and category.
        Consumer consumer = connection.consumerBuilder()
            .queue(ordersQueue)
            .messageHandler((context, message) -> {
                Map<String, Object> annotations = new HashMap<>();
                // This category causes the message to be routed to the business-logic-failures-dlq.
                annotations.put("x-opt-dead-letter-category", "business-logic");
                annotations.put("x-opt-dead-letter-reason", "Customer Not Eligible for Discount");
                System.out.printf("consumer: setting annotations %s and dead lettering...\n", annotations);
                // The Java library will settle the message with the AMQP modified outcome.
                context.discard(annotations);
            }).build();
    }
}
