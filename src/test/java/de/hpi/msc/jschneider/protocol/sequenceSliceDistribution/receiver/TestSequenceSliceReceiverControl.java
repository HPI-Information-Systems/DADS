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

    public void testReceiveSlicePart()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = messageInterface(control);
        val slicePart = range(0, 50);
        val message = SequenceSliceDistributionMessages.SequenceSlicePartMessage.builder()
                                                                                .sender(localSliceDistributor.ref())
                                                                                .receiver(self.ref())
                                                                                .partIndex(0)
                                                                                .isLastPart(false)
                                                                                .slicePart(slicePart)
                                                                                .build();
        messageInterface.apply(message);

        assertThat(writer.getValues()).containsExactly(slicePart);

        val event = expectEvent(SequenceSliceDistributionEvents.SequenceSlicePartReceivedEvent.class);
        assertThat(event.getSlicePart()).containsExactly(slicePart);

        assertThat(control.getModel().getExpectedNextSliceIndex().get()).isOne();
        assertThat(control.getModel().getSliceParts()).isEmpty();

        val ack = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.AcknowledgeSequenceSlicePartMessage.class);
        assertThat(ack.getReceiver()).isEqualTo(localSliceDistributor.ref());

        assertThatMessageIsCompleted(message);
    }

    public void testReceivePartsOutOfOrder()
    {
        val writer = new MockSequenceWriter();
        val control = control(writer);
        val messageInterface = messageInterface(control);
        val slicePart = range(50, 50);
        val message = SequenceSliceDistributionMessages.SequenceSlicePartMessage.builder()
                                                                                .sender(localSliceDistributor.ref())
                                                                                .receiver(self.ref())
                                                                                .partIndex(1)
                                                                                .isLastPart(true)
                                                                                .slicePart(slicePart)
                                                                                .build();
        messageInterface.apply(message);

        assertThat(writer.getValues()).isEmpty();

        assertThat(control.getModel().getSliceParts().size()).isOne();

        val ack = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.AcknowledgeSequenceSlicePartMessage.class);
        assertThat(ack.getReceiver()).isEqualTo(localSliceDistributor.ref());

        assertThatMessageIsCompleted(message);
    }
}
