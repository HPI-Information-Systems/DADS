package de.hpi.msc.jschneider.protocol;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.ProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.common.eventDispatcher.EventDispatcherMessages;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Counter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.PrimitiveMatrixSink;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;
import junit.framework.TestCase;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.structure.Access1D;
import org.ojalgo.type.context.NumberContext;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestCase extends TestCase
{
    protected static final int MATRIX_PRECISION = 5;
    protected static final RoundingMode MATRIX_ROUNDING_MODE = RoundingMode.HALF_UP;
    protected static final NumberContext MATRIX_COMPARISON_CONTEXT = NumberContext.getMath(new MathContext(MATRIX_PRECISION, MATRIX_ROUNDING_MODE));

    protected static final String GRAPH_EDGE_PATTERN_FROM_SEGMENT = "FromSegment";
    protected static final String GRAPH_EDGE_PATTERN_FROM_INDEX = "FromIndex";
    protected static final String GRAPH_EDGE_PATTERN_TO_SEGMENT = "ToSegment";
    protected static final String GRAPH_EDGE_PATTERN_TO_INDEX = "ToIndex";
    protected static final String GRAPH_EDGE_PATTERN_WEIGHT = "Weight";
    protected static final Pattern GRAPH_EDGE_PATTERN = Pattern.compile(String.format("^\\{(?<%1$s>\\d+)_(?<%2$s>\\d+)} -\\[(?<%3$s>\\d+)]-> \\{(?<%4$s>\\d+)_(?<%5$s>\\d+)}$",
                                                                                      GRAPH_EDGE_PATTERN_FROM_SEGMENT,
                                                                                      GRAPH_EDGE_PATTERN_FROM_INDEX,
                                                                                      GRAPH_EDGE_PATTERN_WEIGHT,
                                                                                      GRAPH_EDGE_PATTERN_TO_SEGMENT,
                                                                                      GRAPH_EDGE_PATTERN_TO_INDEX));

    private final List<TestProcessor> processors = new ArrayList<>();

    protected TestProcessor createMaster()
    {
        return createMaster(getProcessorProtocols());
    }

    protected TestProcessor createMaster(ProtocolType... protocolTypes)
    {
        val numberOfMasters = processors.stream().filter(TestProcessor::isMaster).count();
        val processorId = new ProcessorId(String.format("Master%1$d", numberOfMasters), "127.0.0.1", 7788);
        return createProcessor(processorId.toString(), true, protocolTypes);
    }

    protected TestProcessor createSlave()
    {
        return createSlave(getProcessorProtocols());
    }

    protected TestProcessor createSlave(ProtocolType... protocolTypes)
    {
        val numberOfSlaves = processors.stream().filter(processor -> !processor.isMaster()).count();
        val processorId = new ProcessorId(String.format("Master%1$d", numberOfSlaves), "127.0.0.1", 7799);
        return createProcessor(processorId.toString(), false, protocolTypes);
    }

    protected TestProcessor createProcessor(String name, boolean isMaster)
    {
        return createProcessor(name, isMaster, getProcessorProtocols());
    }

    protected abstract ProtocolType[] getProcessorProtocols();

    protected TestProcessor createProcessor(String name, boolean isMaster, ProtocolType... protocols)
    {
        val processor = TestProcessor.create(name);
        processor.setMaster(isMaster);
        for (val protocolType : protocols)
        {
            TestProtocol.create(processor, protocolType);
        }
        processors.add(processor);

        return processor;
    }

    protected <TModel extends ProtocolParticipantModel> TModel finalizeModel(TModel model, TestProbe self)
    {
        model.setSelfProvider(self::ref);
        model.setSenderProvider(ActorRef::noSender);
        model.setProcessorProvider(() -> processors.toArray(new Processor[0]));
        model.setMaximumMessageSizeProvider(() -> 1024 * 1024 * 10L); // 10 MiB
        model.setWatchActorCallback(subject ->
                                    {
                                    });
        model.setUnwatchActorCallback(subject ->
                                      {
                                      });
        model.setChildFactory((props, name) -> ActorRef.noSender());

        return model;
    }

    protected <TModel extends ProtocolParticipantModel, TControl extends ProtocolParticipantControl<TModel>> PartialFunction<Object, BoxedUnit> createMessageInterface(TControl control)
    {
        val receiveBuilder = new ImprovedReceiveBuilder();
        control.complementReceiveBuilder(receiveBuilder);

        return receiveBuilder.build().onMessage();
    }

    protected MessageExchangeMessages.MessageCompletedMessage assertThatMessageIsCompleted(MessageExchangeMessages.MessageExchangeMessage message, TestProcessor processor)
    {
        val messageDispatcher = processor.getProtocolRootActor(ProtocolType.MessageExchange);
        val completedMessage = messageDispatcher.expectMsgClass(MessageExchangeMessages.MessageCompletedMessage.class);

        assertThat(completedMessage.getCompletedMessageId()).isEqualTo(message.getId());

        return completedMessage;
    }

    protected EventDispatcherMessages.SubscribeToEventMessage assertEventSubscription(Class<?> eventType, TestProcessor processor)
    {
        val messageDispatcher = processor.getProtocolRootActor(ProtocolType.MessageExchange);
        val subscription = messageDispatcher.expectMsgClass(EventDispatcherMessages.SubscribeToEventMessage.class);

        assertThat(subscription.getEventType()).isEqualTo(eventType);

        return subscription;
    }

    protected <TEvent extends MessageExchangeMessages.RedirectableMessage> TEvent expectEvent(Class<TEvent> eventType, TestProcessor processor)
    {
        val messageDispatcher = processor.getProtocolRootActor(ProtocolType.MessageExchange);
        return messageDispatcher.expectMsgClass(eventType);
    }

    protected MatrixStore<Double> createMatrix(long rows, long columns)
    {
        val matrixInitializer = new MatrixInitializer(columns);
        val random = new Random();

        for (var rowIndex = 0L; rowIndex < rows; ++rowIndex)
        {
            val row = new float[(int) columns];
            for (var columnIndex = 0; columnIndex < columns; ++columnIndex)
            {
                val bigDecimal = new BigDecimal(Float.toString(random.nextFloat())).setScale(MATRIX_PRECISION, MATRIX_ROUNDING_MODE);
                row[columnIndex] = bigDecimal.floatValue();
            }
            matrixInitializer.appendRow(row);
        }

        return matrixInitializer.create();
    }

    protected PrimitiveMatrixSink performDataTransfer(TestProbe dataReceiver, PartialFunction<Object, BoxedUnit> dataReceiverMessageInterface,
                                                      TestProbe dataSender, PartialFunction<Object, BoxedUnit> dataSenderMessageInterface,
                                                      DataTransferMessages.InitializeDataTransferMessage initializeDataTransferMessage,
                                                      boolean expectFinalMessageCompletion)
    {
        val receiverProcessor = processors.stream().filter(processor -> processor.getId().equals(ProcessorId.of(dataReceiver.ref()))).findFirst();
        assert receiverProcessor.isPresent() : "Unable to find receiverProcessor!";

        val senderProcessor = processors.stream().filter(processor -> processor.getId().equals(ProcessorId.of(dataSender.ref()))).findFirst();
        assert senderProcessor.isPresent() : "Unable to find senderProcessor!";

        val sink = new PrimitiveMatrixSink();
        val operationId = initializeDataTransferMessage.getOperationId();

        MessageExchangeMessages.MessageExchangeMessage nextMessageToCompleteOnReceiver = initializeDataTransferMessage;
        dataReceiverMessageInterface.apply(initializeDataTransferMessage);

        while (true)
        {
            val requestNextPart = receiverProcessor.get().getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
            assertThat(requestNextPart.getOperationId()).isEqualTo(operationId);
            dataSenderMessageInterface.apply(requestNextPart);
            assertThatMessageIsCompleted(nextMessageToCompleteOnReceiver, receiverProcessor.get());

            val part = senderProcessor.get().getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.DataPartMessage.class);
            sink.write(part.getPart());
            dataReceiverMessageInterface.apply(part);
            assertThatMessageIsCompleted(requestNextPart, senderProcessor.get());

            nextMessageToCompleteOnReceiver = part;

            if (!part.isLastPart())
            {
                continue;
            }

            if (expectFinalMessageCompletion)
            {
                assertThatMessageIsCompleted(part, receiverProcessor.get());
            }

            sink.close();
            break;
        }

        return sink;
    }

    protected void transfer(Access1D<Double> data,
                            TestProbe dataReceiver, PartialFunction<Object, BoxedUnit> dataReceiverMessageInterface,
                            DataTransferMessages.InitializeDataTransferMessage initializeDataTransferMessage,
                            boolean expectFinalMessageCompletion)
    {
        val receiverProcessor = processors.stream().filter(processor -> processor.getId().equals(ProcessorId.of(dataReceiver.ref()))).findFirst();
        assert receiverProcessor.isPresent() : "Unable to find receiverProcessor!";

        val operationId = initializeDataTransferMessage.getOperationId();

        val source = GenericDataSource.create(data);

        MessageExchangeMessages.MessageExchangeMessage nextMessageToComplete = initializeDataTransferMessage;
        dataReceiverMessageInterface.apply(initializeDataTransferMessage);

        while (true)
        {
            val request = receiverProcessor.get().getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
            assertThat(request.getOperationId()).isEqualTo(operationId);

            assertThatMessageIsCompleted(nextMessageToComplete, receiverProcessor.get());

            val values = source.read(1024);
            val isLastPart = source.isAtEnd();

            val part = DataTransferMessages.DataPartMessage.builder()
                                                           .receiver(dataReceiver.ref())
                                                           .sender(initializeDataTransferMessage.getSender())
                                                           .operationId(operationId)
                                                           .part(values)
                                                           .isLastPart(isLastPart)
                                                           .build();
            dataReceiverMessageInterface.apply(part);
            nextMessageToComplete = part;

            if (!isLastPart)
            {
                continue;
            }

            if (expectFinalMessageCompletion)
            {
                assertThatMessageIsCompleted(nextMessageToComplete, receiverProcessor.get());
            }

            break;
        }
    }

    protected Graph createGraph(int numberOfEdges, int numberOfIntersectionSegments)
    {
        val graph = new Graph();
        for (var edgeIndex = 0; edgeIndex < numberOfEdges; ++edgeIndex)
        {
            val intersectionSegment = edgeIndex % numberOfIntersectionSegments;
            val nextIntersectionSegment = (edgeIndex + 1) % numberOfIntersectionSegments;
            graph.addEdge(edgeIndex,
                          createGraphEdges(String.format("{%1$d_%2$d} -[1]-> {%3$d_%4$d}",
                                                         intersectionSegment,
                                                         edgeIndex,
                                                         nextIntersectionSegment,
                                                         edgeIndex + 1))[0]);
        }

        return graph;
    }

    protected GraphEdge[] createGraphEdges(String... edges)
    {
        val parsedEdges = new GraphEdge[edges.length];
        for (var edgesIndex = 0; edgesIndex < edges.length; ++edgesIndex)
        {
            val matcher = GRAPH_EDGE_PATTERN.matcher(edges[edgesIndex]);
            assertThat(matcher.matches()).isTrue();

            parsedEdges[edgesIndex] = GraphEdge.builder()
                                               .from(GraphNode.builder()
                                                              .intersectionSegment(Integer.parseInt(matcher.group(GRAPH_EDGE_PATTERN_FROM_SEGMENT)))
                                                              .index(Integer.parseInt(matcher.group(GRAPH_EDGE_PATTERN_FROM_INDEX)))
                                                              .build())
                                               .to(GraphNode.builder()
                                                            .intersectionSegment(Integer.parseInt(matcher.group(GRAPH_EDGE_PATTERN_TO_SEGMENT)))
                                                            .index(Integer.parseInt(matcher.group(GRAPH_EDGE_PATTERN_TO_INDEX)))
                                                            .build())
                                               .weight(new Counter(Long.parseLong(matcher.group(GRAPH_EDGE_PATTERN_WEIGHT))))
                                               .build();
        }

        return parsedEdges;
    }

    protected NodeCreationEvents.ResponsibilitiesReceivedEvent createResponsibilitiesReceivedEvent(TestProbe sender,
                                                                                                   TestProbe receiver,
                                                                                                   int numberOfIntersectionSegments,
                                                                                                   long numberOfSubSequences,
                                                                                                   TestProbe... participants)
    {
        return NodeCreationEvents.ResponsibilitiesReceivedEvent.builder()
                                                               .sender(sender.ref())
                                                               .receiver(receiver.ref())
                                                               .numberOfIntersectionSegments(numberOfIntersectionSegments)
                                                               .segmentResponsibilities(createIntersectionSegmentResponsibilities(numberOfIntersectionSegments, participants))
                                                               .subSequenceResponsibilities(createSubSequenceResponsibilities(numberOfSubSequences, participants))
                                                               .build();
    }

    protected Map<ProcessorId, Int64Range> createSubSequenceResponsibilities(long numberOfSubSequences, TestProbe... participants)
    {
        val responsibilities = new HashMap<ProcessorId, Int64Range>();
        val subSequencesPerParticipant = Math.ceil(numberOfSubSequences / (double) participants.length);
        for (var participantsIndex = 0; participantsIndex < participants.length; ++participantsIndex)
        {
            val participant = participants[participantsIndex];
            responsibilities.put(ProcessorId.of(participant.ref()),
                                 Int64Range.builder()
                                           .from((long) (participantsIndex * subSequencesPerParticipant))
                                           .to((long) Math.min(numberOfSubSequences, (participantsIndex + 1) * subSequencesPerParticipant))
                                           .build());
        }

        return responsibilities;
    }

    protected Map<ProcessorId, Int32Range> createIntersectionSegmentResponsibilities(int numberOfIntersectionSegments, TestProbe... participants)
    {
        val responsibilities = new HashMap<ProcessorId, Int32Range>();
        val segmentsPerParticipant = Math.ceil(numberOfIntersectionSegments / (double) participants.length);
        for (var participantsIndex = 0; participantsIndex < participants.length; ++participantsIndex)
        {
            val participant = participants[participantsIndex];
            responsibilities.put(ProcessorId.of(participant.ref()),
                                 Int32Range.builder()
                                           .from((int) (participantsIndex * segmentsPerParticipant))
                                           .to((int) Math.min(numberOfIntersectionSegments, (participantsIndex + 1) * segmentsPerParticipant))
                                           .build());
        }

        return responsibilities;
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();

        for (val processor : processors)
        {
            processor.terminate();
        }
        processors.clear();
    }
}
