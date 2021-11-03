package io.github.stavshamir.springwolf.asyncapi.scanners.channels;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.github.stavshamir.springwolf.asyncapi.scanners.components.ComponentsScanner;
import io.github.stavshamir.springwolf.asyncapi.types.channel.Channel;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.Operation;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.bindings.OperationBinding;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.bindings.kafka.KafkaOperationBinding;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.message.Message;
import io.github.stavshamir.springwolf.asyncapi.types.channel.operation.message.PayloadReference;
import io.github.stavshamir.springwolf.configuration.KafkaProtocolConfiguration;
import io.github.stavshamir.springwolf.schemas.DefaultSchemasService;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {KafkaChannelsScanner.class, DefaultSchemasService.class})
@TestPropertySource(properties = "kafka.topics.test=test-topic")
public class KafkaChannelsScannerTest {

    @Autowired
    private KafkaChannelsScanner kafkaChannelsScanner;

    @MockBean
    private ComponentsScanner componentsScanner;

    @MockBean
    private KafkaProtocolConfiguration kafkaProtocolConfiguration;

    @Value("${kafka.topics.test}")
    private String topicFromProperties;

    private static final String TOPIC = "test-topic";

    @Before
    public void setUp() throws Exception {
        when(kafkaProtocolConfiguration.getBasePackage())
                .thenReturn("Does not matter - will be set by component scanner mock");
    }

    private void setClassToScan(Class<?> classToScan) {
        Set<Class<?>> classesToScan = singleton(classToScan);
        when(componentsScanner.scanForComponents(anyString())).thenReturn(classesToScan);
    }

    @Test
    public void scan_componentHasNoKafkaListenerMethods() {
        setClassToScan(ClassWithoutKafkaListenerAnnotations.class);

        Map<String, Channel> channels = kafkaChannelsScanner.scan();

        assertThat(channels)
                .isEmpty();
    }

    @Test
    public void scan_componentHasKafkaListenerMethods_hardCodedTopic() {
        // Given a class with methods annotated with KafkaListener, whose topics attribute is hard coded
        setClassToScan(ClassWithKafkaListenerAnnotationHardCodedTopic.class);

        // When scan is called
        Map<String, Channel> actualChannels = kafkaChannelsScanner.scan();

        // Then the returned collection contains the channel
        Message message = Message.builder()
                .name(SimpleFoo.class.getName())
                .title(SimpleFoo.class.getSimpleName())
                .payload(PayloadReference.fromModelName(SimpleFoo.class.getSimpleName()))
                .build();

        Operation operation = Operation.builder()
                .bindings(ImmutableMap.of("kafka", KafkaOperationBinding.withGroupId("")))
                .message(message)
                .build();

        Channel expectedChannel = Channel.builder().publish(operation).build();

        assertThat(actualChannels)
                .containsExactly(Maps.immutableEntry(TOPIC, expectedChannel));
    }

    @Test
    public void scan_componentHasKafkaListenerMethods_embeddedValueTopic() {
        // Given a class with methods annotated with KafkaListener, whose topics attribute is an embedded value
        setClassToScan(ClassWithKafkaListenerAnnotationsEmbeddedValueTopic.class);

        // When scan is called
        Map<String, Channel> actualChannels = kafkaChannelsScanner.scan();

        // Then the returned collection contains the channel
        Message message = Message.builder()
                .name(SimpleFoo.class.getName())
                .title(SimpleFoo.class.getSimpleName())
                .payload(PayloadReference.fromModelName(SimpleFoo.class.getSimpleName()))
                .build();

        Operation operation = Operation.builder()
                .bindings(ImmutableMap.of("kafka", KafkaOperationBinding.withGroupId("")))
                .message(message)
                .build();

        Channel expectedChannel = Channel.builder().publish(operation).build();

        assertThat(actualChannels)
                .containsExactly(Maps.immutableEntry(TOPIC, expectedChannel));
    }

    @Test
    public void scan_componentHasKafkaListenerMethods_withGroupId() {
        // Given a class with methods annotated with KafkaListener, with a group id
        setClassToScan(ClassWithKafkaListenerAnnotationWithGroupId.class);

        // When scan is called
        Map<String, Channel> actualChannels = kafkaChannelsScanner.scan();

        // Then the returned collection contains a correct binding
        Map<String, ? extends OperationBinding> actualBindings = actualChannels.get(TOPIC)
                .getPublish()
                .getBindings();

        List<String> expectedBinding = KafkaOperationBinding
                .withGroupId(ClassWithKafkaListenerAnnotationWithGroupId.GROUP_ID)
                .getGroupId()
                .get_enum();

        assertThat(actualBindings).isNotNull();
        assertThat(actualBindings.get("kafka")).isNotNull();
        assertThat(expectedBinding)
                .isEqualTo(Collections.singletonList(ClassWithKafkaListenerAnnotationWithGroupId.GROUP_ID));
    }

    @Test
    public void scan_componentHasKafkaListenerMethods_multipleParamsWithoutPayloadAnnotation() {
        // Given a class with a method annotated with KafkaListener:
        // - The method has more than one parameter
        // - No parameter is annotated with @Payload
        setClassToScan(ClassWithKafkaListenerAnnotationMultipleParamsWithoutPayloadAnnotation.class);

        // Then an exception is thrown when scan is called
        assertThatThrownBy(() -> kafkaChannelsScanner.scan())
                .isInstanceOf(IllegalArgumentException.class);
    }


    @Test
    public void scan_componentHasKafkaListenerMethods_multipleParamsWithPayloadAnnotation() {
        // Given a class with a method annotated with KafkaListener:
        // - The method has more than one parameter
        // - There is a parameter is annotated with @Payload
        setClassToScan(ClassWithKafkaListenerAnnotationMultipleParamsWithPayloadAnnotation.class);

        // When scan is called
        Map<String, Channel> actualChannels = kafkaChannelsScanner.scan();

        // Then the returned collection contains the channel, and the payload is of the parameter annotated with @Payload
        Message message = Message.builder()
                .name(SimpleFoo.class.getName())
                .title(SimpleFoo.class.getSimpleName())
                .payload(PayloadReference.fromModelName(SimpleFoo.class.getSimpleName()))
                .build();

        Operation operation = Operation.builder()
                .bindings(ImmutableMap.of("kafka", KafkaOperationBinding.withGroupId("")))
                .message(message)
                .build();

        Channel expectedChannel = Channel.builder().publish(operation).build();

        assertThat(actualChannels)
                .containsExactly(Maps.immutableEntry(TOPIC, expectedChannel));
    }

    private static class ClassWithoutKafkaListenerAnnotations {

        private void methodWithoutAnnotation() {
        }

    }

    private static class ClassWithKafkaListenerAnnotationHardCodedTopic {

        @KafkaListener(topics = TOPIC)
        private void methodWithAnnotation(SimpleFoo payload) {
        }

        private void methodWithoutAnnotation() {
        }

    }

    private static class ClassWithKafkaListenerAnnotationsEmbeddedValueTopic {

        @KafkaListener(topics = "${kafka.topics.test}")
        private void methodWithAnnotation1(SimpleFoo payload) {
        }

    }

    private static class ClassWithKafkaListenerAnnotationWithGroupId {

        private static final String GROUP_ID = "test-group-id";

        @KafkaListener(topics = TOPIC, groupId = GROUP_ID)
        private void methodWithAnnotation(SimpleFoo payload) {
        }

        private void methodWithoutAnnotation() {
        }

    }

    private static class ClassWithKafkaListenerAnnotationMultipleParamsWithoutPayloadAnnotation {

        @KafkaListener(topics = TOPIC)
        private void methodWithAnnotation(SimpleFoo payload, String anotherParam) {
        }

    }

    private static class ClassWithKafkaListenerAnnotationMultipleParamsWithPayloadAnnotation {

        @KafkaListener(topics = TOPIC)
        private void methodWithAnnotation(String anotherParam, @Payload SimpleFoo payload) {
        }

    }

    @Data
    @NoArgsConstructor
    private static class SimpleFoo {
        private String s;
        private boolean b;
    }

}