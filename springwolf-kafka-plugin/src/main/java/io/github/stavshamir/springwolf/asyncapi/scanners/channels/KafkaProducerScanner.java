package io.github.stavshamir.springwolf.asyncapi.scanners.channels;

import com.google.common.collect.Maps;
import io.github.stavshamir.springwolf.asyncapi.scanners.components.ComponentsScanner;
import io.github.stavshamir.springwolf.asyncapi.types.ProducerData;
import io.github.stavshamir.springwolf.asyncapi.types.channel.Channel;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.Operation;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.bindings.OperationBinding;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.bindings.kafka.KafkaOperationBinding;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.message.Message;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.message.PayloadReference;
import io.github.stavshamir.springwolf.configuration.KafkaProtocolConfiguration;
import io.github.stavshamir.springwolf.producer.AsyncApiEventProducer;
import io.github.stavshamir.springwolf.schemas.SchemasService;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;

import static java.util.stream.Collectors.*;

@RequiredArgsConstructor
@Slf4j
@Service
public class KafkaProducerScanner {

    private final KafkaProtocolConfiguration kafkaProtocolConfiguration;
    private final ComponentsScanner componentsScanner;
    private final SchemasService schemasService;
    private Class<? extends Annotation> annotation = AsyncApiEventProducer.class;
    @Getter
    private List<ProducerData> producerData = new ArrayList<>();

    @PostConstruct
    public void scan() {
        producerData = componentsScanner.scanForComponents(kafkaProtocolConfiguration.getBasePackage())
                .stream()
                .map(this::getAnnotatedMethods)
                .flatMap(Collection::stream)
                .map(this::mapMethodToChannel)
                .map(e -> {
                    try {
                        return ProducerData.builder()
                                .channelName(e.getKey())
                                .binding(Map.of("kafka", new KafkaOperationBinding()))
                                .payloadType(Class.forName(e.getValue().getPublish().getMessage().getName()))
                                .build();
                    } catch (ClassNotFoundException ex) {
                        ex.printStackTrace();
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());
    }

    private Set<Method> getAnnotatedMethods(Class<?> type) {
        Class<? extends Annotation> annotationClass = annotation;
        log.debug("Scanning class \"{}\" for @\"{}\" annotated methods", type.getName(), annotationClass.getName());

        return Arrays.stream(type.getDeclaredMethods())
                .filter(method -> !method.isBridge())
                .filter(method -> method.isAnnotationPresent(annotationClass))
                .collect(toSet());
    }

    private Map.Entry<String, Channel> mapMethodToChannel(Method method) {
        log.debug("Mapping method \"{}\" to channels", method.getName());

        Class<? extends Annotation> listenerAnnotationClass = annotation;
        Annotation annotation = Optional.of(method.getAnnotation(listenerAnnotationClass))
                .orElseThrow(() -> new IllegalArgumentException("Method must be annotated with " + listenerAnnotationClass.getName()));

        String channelName = getChannelName((AsyncApiEventProducer) annotation);

        Map<String, ? extends OperationBinding> operationBinding = buildOperationBinding((AsyncApiEventProducer) annotation);
        Class<?> payload = getPayloadType(method);
        Channel channel = buildChannel(payload, operationBinding);

        return Maps.immutableEntry(channelName, channel);
    }

    protected String getChannelName(AsyncApiEventProducer annotation) {
        return annotation.topic();
    }

    protected Map<String, ? extends OperationBinding> buildOperationBinding(AsyncApiEventProducer annotation) {

        KafkaOperationBinding binding = new KafkaOperationBinding();
        return Map.of(KafkaOperationBinding.KAFKA_BINDING_KEY, binding);
    }

    protected Class<?> getPayloadType(Method method) {
        String methodName = String.format("%s::%s", method.getDeclaringClass().getSimpleName(), method.getName());
        log.debug("Finding payload type for {}", methodName);

        Class<?>[] parameterTypes = method.getParameterTypes();
        switch (parameterTypes.length) {
            case 0:
                throw new IllegalArgumentException("Listener methods must not have 0 parameters: " + methodName);
            case 1:
                return parameterTypes[0];
            default:
                return getPayloadType(parameterTypes, method.getParameterAnnotations(), methodName);
        }
    }

    private Class<?> getPayloadType(Class<?>[] parameterTypes, Annotation[][] parameterAnnotations, String methodName) {
        int payloadAnnotatedParameterIndex = getPayloadAnnotatedParameterIndex(parameterAnnotations);

        if (payloadAnnotatedParameterIndex == -1) {
            String msg = "Multi-parameter KafkaListener methods must have one parameter annotated with @Payload, "
                    + "but none was found: "
                    + methodName;

            throw new IllegalArgumentException(msg);
        }

        return parameterTypes[payloadAnnotatedParameterIndex];
    }

    private int getPayloadAnnotatedParameterIndex(Annotation[][] parameterAnnotations) {
        for (int i = 0, length = parameterAnnotations.length; i < length; i++) {
            Annotation[] annotations = parameterAnnotations[i];
            boolean hasPayloadAnnotation = Arrays.stream(annotations)
                    .anyMatch(annotation -> annotation instanceof Payload);

            if (hasPayloadAnnotation) {
                return i;
            }
        }

        return -1;
    }

    private Channel buildChannel(Class<?> payloadType, Map<String, ? extends OperationBinding> operationBinding) {
        String modelName = schemasService.register(payloadType);

        Message message = Message.builder()
                .name(payloadType.getName())
                .title(modelName)
                .payload(PayloadReference.fromModelName(modelName))
                .build();

        Operation operation = Operation.builder()
                .message(message)
                .bindings(operationBinding)
                .build();

        return Channel.builder()
                .publish(operation)
                .build();
    }
}
