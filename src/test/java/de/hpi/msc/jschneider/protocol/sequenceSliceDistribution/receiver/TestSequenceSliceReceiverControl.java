package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.fileHandling.MockSequenceWriter;
import de.hpi.msc.jschneider.fileHandling.writing.SequenceWriter;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
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

        var request = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
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
                                                              .part(slicePart)
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
                                                              .part(slicePart)
                                                              .operationId(OPERATION_ID)
                                                              .build();
        messageInterface.apply(partMessage);

        val event = expectEvent(SequenceSliceDistributionEvents.ProjectionCreatedEvent.class);
        assertThat(event.getFirstSubSequenceIndex()).isEqualTo(10L);
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
                                                              .part(slicePart)
                                                              .operationId(OPERATION_ID)
                                                              .build();
        messageInterface.apply(partMessage);

        assertThat(writer.getValues()).isEmpty();

        val request = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThat(request.getReceiver()).isEqualTo(localSliceDistributor.ref());

        assertThatMessageIsCompleted(partMessage);
    }
}
