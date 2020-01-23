package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.fileHandling.MockSequenceWriter;
import de.hpi.msc.jschneider.fileHandling.writing.SequenceWriter;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;
import lombok.var;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSequenceSliceReceiverControl extends ProtocolTestCase
{
    private static final UUID OPERATION_ID = UUID.randomUUID();

    private TestProbe localSliceDistributor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        localSliceDistributor = localProcessor.createActor("SliceDistributor");
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.SequenceSliceDistribution};
    }

    private SequenceSliceReceiverModel model(SequenceWriter writer)
    {
        return finalizeModel(SequenceSliceReceiverModel.builder()
                                                       .sequenceWriter(writer)
                                                       .build());
    }

    private SequenceSliceReceiverControl control(SequenceWriter writer)
    {
        return new SequenceSliceReceiverControl(model(writer));
    }

    private void initialize(SequenceSliceReceiverControl control, PartialFunction<Object, BoxedUnit> messageInterface)
    {
        val message = SequenceSliceDistributionMessages.InitializeSequenceSliceTransferMessage.builder()
                                                                                              .sender(localSliceDistributor.ref())
                                                                                              .receiver(self.ref())
                                                                                              .firstSubSequenceIndex(10L)
                                                                                              .subSequenceLength(10)
                                                                                              .convolutionSize(3)
                                                                                              .operationId(OPERATION_ID)
                                                                                              .build();
        messageInterface.apply(message);

        assertThat(control.getModel().getFirstSubSequenceIndex()).isEqualTo(10L);
        assertThat(control.getModel().getSubSequenceLength()).isEqualTo(10);
        assertThat(control.getModel().getConvolutionSize()).isEqualTo(3);
        assertThat(control.getModel().getProjectionInitializer()).isNotNull();

        val parametersReceivedEvent = expectEvent(SequenceSliceDistributionEvents.SubSequenceParametersReceivedEvent.class);
        assertThat(parametersReceivedEvent.getSubSequenceLength()).isEqualTo(control.getModel().getSubSequenceLength());
        assertThat(parametersReceivedEvent.getConvolutionSize()).isEqualTo(control.getModel().getConvolutionSize());
        assertThat(parametersReceivedEvent.getFirstSubSequenceIndex()).isEqualTo(control.getModel().getFirstSubSequenceIndex());
        assertThat(parametersReceivedEvent.isLastSubSequenceChunk()).isEqualTo(control.getModel().isLastSubSequenceChunk());

        val request = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThat(request.getReceiver()).isEqualTo(localSliceDistributor.ref());

        assertThatMessageIsCompleted(message);
    }

    private float[] range(int start, int length)
    {
        val values = new float[length];
        for (var i = 0; i < length; ++i)
        {
            values[i] = (float) i + start;
        }

        return values;
    }

    public void testInitializeTransfer()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = createMessageInterface(control);

        initialize(control, messageInterface);
    }

    public void testReceiveSlicePart()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = createMessageInterface(control);
        val slicePart = range(0, 50);

        initialize(control, messageInterface);

        var partMessage = DataTransferMessages.DataPartMessage.builder()
                                                              .sender(localSliceDistributor.ref())
                                                              .receiver(self.ref())
                                                              .isLastPart(false)
                                                              .part(Serialize.toBytes(slicePart))
                                                              .operationId(OPERATION_ID)
                                                              .build();
        messageInterface.apply(partMessage);

        assertThat(writer.getValues()).containsExactly(slicePart);

        val request = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThat(request.getReceiver()).isEqualTo(localSliceDistributor.ref());

        assertThatMessageIsCompleted(partMessage);
    }

    public void testReceiveLastSlicePart()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = createMessageInterface(control);
        val slicePart = range(0, 50);

        initialize(control, messageInterface);

        var partMessage = DataTransferMessages.DataPartMessage.builder()
                                                              .sender(localSliceDistributor.ref())
                                                              .receiver(self.ref())
                                                              .isLastPart(true)
                                                              .part(Serialize.toBytes(slicePart))
                                                              .operationId(OPERATION_ID)
                                                              .build();
        messageInterface.apply(partMessage);

        val event = expectEvent(SequenceSliceDistributionEvents.ProjectionCreatedEvent.class);
        assertThat(event.getFirstSubSequenceIndex()).isEqualTo(10L);
        assertThat(event.getMinimumRecord()).isEqualTo(slicePart[0]);
        assertThat(event.getMaximumRecord()).isEqualTo(slicePart[slicePart.length - 1]);
        assertThat(event.getProjection().countColumns()).isEqualTo(7L);
        assertThat(event.getProjection().countRows()).isEqualTo(41L);

        assertThat(writer.getValues()).containsExactly(slicePart);
        assertThatMessageIsCompleted(partMessage);
    }

    public void testReceiveEmptySlicePart()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = createMessageInterface(control);
        val slicePart = new float[0];

        initialize(control, messageInterface);

        var partMessage = DataTransferMessages.DataPartMessage.builder()
                                                              .sender(localSliceDistributor.ref())
                                                              .receiver(self.ref())
                                                              .isLastPart(false)
                                                              .part(Serialize.toBytes(slicePart))
                                                              .operationId(OPERATION_ID)
                                                              .build();
        messageInterface.apply(partMessage);

        assertThat(writer.getValues()).isEmpty();

        val request = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThat(request.getOperationId()).isEqualTo(OPERATION_ID);
        assertThat(request.getReceiver()).isEqualTo(localSliceDistributor.ref());

        assertThatMessageIsCompleted(partMessage);
    }

    public void testCreateProjection()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = createMessageInterface(control);
        val slicePart = range(0, 10);

        val subSequenceLength = 5;
        val convolutionSize = 3;
        val slidingWindowWidth = subSequenceLength - convolutionSize;

        val initialize = SequenceSliceDistributionMessages.InitializeSequenceSliceTransferMessage.builder()
                                                                                                 .sender(localSliceDistributor.ref())
                                                                                                 .receiver(self.ref())
                                                                                                 .firstSubSequenceIndex(0L)
                                                                                                 .subSequenceLength(subSequenceLength)
                                                                                                 .convolutionSize(convolutionSize)
                                                                                                 .operationId(OPERATION_ID)
                                                                                                 .build();
        messageInterface.apply(initialize);

        val parametersReceivedEvent = expectEvent(SequenceSliceDistributionEvents.SubSequenceParametersReceivedEvent.class);
        assertThat(parametersReceivedEvent.getSubSequenceLength()).isEqualTo(control.getModel().getSubSequenceLength());
        assertThat(parametersReceivedEvent.getConvolutionSize()).isEqualTo(control.getModel().getConvolutionSize());
        assertThat(parametersReceivedEvent.getFirstSubSequenceIndex()).isEqualTo(control.getModel().getFirstSubSequenceIndex());
        assertThat(parametersReceivedEvent.isLastSubSequenceChunk()).isEqualTo(control.getModel().isLastSubSequenceChunk());

        val request = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThat(request.getOperationId()).isEqualTo(OPERATION_ID);
        assertThat(request.getReceiver()).isEqualTo(localSliceDistributor.ref());
        assertThatMessageIsCompleted(initialize);

        val part = DataTransferMessages.DataPartMessage.builder()
                                                       .sender(localSliceDistributor.ref())
                                                       .receiver(self.ref())
                                                       .isLastPart(true)
                                                       .part(Serialize.toBytes(slicePart))
                                                       .operationId(OPERATION_ID)
                                                       .build();
        messageInterface.apply(part);

        val projectionCreated = expectEvent(SequenceSliceDistributionEvents.ProjectionCreatedEvent.class);
        assertThat(projectionCreated.getProjection().countColumns()).isEqualTo(slidingWindowWidth);
        assertThat(projectionCreated.getProjection().countRows()).isEqualTo(slicePart.length - (subSequenceLength - 1));

        val expectedProjection = (new MatrixInitializer(2L)
                                          .appendRow(new float[]{0.0f + 1.0f + 2.0f, 1.0f + 2.0f + 3.0f})
                                          .appendRow(new float[]{1.0f + 2.0f + 3.0f, 2.0f + 3.0f + 4.0f})
                                          .appendRow(new float[]{2.0f + 3.0f + 4.0f, 3.0f + 4.0f + 5.0f})
                                          .appendRow(new float[]{3.0f + 4.0f + 5.0f, 4.0f + 5.0f + 6.0f})
                                          .appendRow(new float[]{4.0f + 5.0f + 6.0f, 5.0f + 6.0f + 7.0f})
                                          .appendRow(new float[]{5.0f + 6.0f + 7.0f, 6.0f + 7.0f + 8.0f})
                                          .create());

        assertThat(projectionCreated.getProjection().equals(expectedProjection, MATRIX_COMPARISON_CONTEXT)).isTrue();

        assertThatMessageIsCompleted(part);
    }
}
