package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.fileHandling.MockSequenceWriter;
import de.hpi.msc.jschneider.fileHandling.writing.SequenceWriter;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import lombok.val;
import lombok.var;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSequenceSliceReceiverControl extends ProtocolTestCase
{
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
        val messageInterface = messageInterface(control);

        val initialize = SequenceSliceDistributionMessages.InitializeSliceTransferMessage.builder()
                                                                                         .sender(localSliceDistributor.ref())
                                                                                         .receiver(self.ref())
                                                                                         .firstSubSequenceIndex(10L)
                                                                                         .subSequenceLength(10)
                                                                                         .build();
        messageInterface.apply(initialize);

        assertThat(control.getModel().getNextSubSequenceIndex().get()).isEqualTo(10L);
        assertThat(control.getModel().getSubSequenceLength()).isEqualTo(10);

        var request = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.RequestNextSlicePartMessage.class);
        assertThat(request.getReceiver()).isEqualTo(localSliceDistributor.ref());

        assertThatMessageIsCompleted(initialize);
    }

    public void testReceiveSlicePart()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = messageInterface(control);
        val slicePart = range(0, 50);

        var slicePartMessage = SequenceSliceDistributionMessages.SequenceSlicePartMessage.builder()
                                                                                         .sender(localSliceDistributor.ref())
                                                                                         .receiver(self.ref())
                                                                                         .isLastPart(false)
                                                                                         .slicePart(slicePart)
                                                                                         .build();
        messageInterface.apply(slicePartMessage);

        val event = expectEvent(SequenceSliceDistributionEvents.SequenceSlicePartReceivedEvent.class);
        assertThat(event.getSlicePart()).containsExactly(slicePart);

        assertThat(writer.getValues()).containsExactly(slicePart);

        val request = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.RequestNextSlicePartMessage.class);
        assertThat(request.getReceiver()).isEqualTo(localSliceDistributor.ref());

        assertThatMessageIsCompleted(slicePartMessage);
    }

    public void testReceiveLastSlicePart()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = messageInterface(control);
        val slicePart = range(0, 50);

        var slicePartMessage = SequenceSliceDistributionMessages.SequenceSlicePartMessage.builder()
                                                                                         .sender(localSliceDistributor.ref())
                                                                                         .receiver(self.ref())
                                                                                         .isLastPart(true)
                                                                                         .slicePart(slicePart)
                                                                                         .build();
        messageInterface.apply(slicePartMessage);

        val event = expectEvent(SequenceSliceDistributionEvents.SequenceSlicePartReceivedEvent.class);
        assertThat(event.getSlicePart()).containsExactly(slicePart);

        assertThat(writer.getValues()).containsExactly(slicePart);
        assertThatMessageIsCompleted(slicePartMessage);
    }

    public void testReceiveEmptySlicePart()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = messageInterface(control);
        val slicePart = new float[0];

        var slicePartMessage = SequenceSliceDistributionMessages.SequenceSlicePartMessage.builder()
                                                                                         .sender(localSliceDistributor.ref())
                                                                                         .receiver(self.ref())
                                                                                         .isLastPart(false)
                                                                                         .slicePart(slicePart)
                                                                                         .build();
        messageInterface.apply(slicePartMessage);

        assertThat(writer.getValues()).isEmpty();
        assertThatMessageIsCompleted(slicePartMessage);
    }
}
