package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.fileHandling.MockSequenceReader;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import lombok.val;
import lombok.var;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSequenceSliceDistributorControl extends ProtocolTestCase
{
    private final int SEQUENCE_SIZE = 100;

    private TestProbe localSliceReceiver;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        localSliceReceiver = localProcessor.createActor("sliceReceiver");
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.SequenceSliceDistribution};
    }

    private SequenceSliceDistributorModel dummyModel()
    {
        return finalizeModel(SequenceSliceDistributorModel.builder()
                                                          .sliceReceiverActorSystem(localProcessor.getRootPath())
                                                          .sequenceReader(new MockSequenceReader(SEQUENCE_SIZE))
                                                          .maximumMessageSizeProvider(() -> (long) (SEQUENCE_SIZE / 2))
                                                          .build());
    }

    private SequenceSliceDistributorControl control()
    {
        return new SequenceSliceDistributorControl(dummyModel());
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

    public void testPreStartSendsFirstSlicePart()
    {
        val control = control();
        control.getModel().setMaximumMessageSizeProvider(() -> (long) SEQUENCE_SIZE * Float.BYTES);
        control.getModel().setSliceSizeFactor(1.0f);

        control.preStart();

        val message = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(message.getPartIndex()).isZero();
        assertThat(message.isLastPart()).isTrue();
        assertThat(message.getSlicePart()).containsExactly(range(0, 100));
        assertThat(message.getReceiver()).isEqualTo(localProcessor.getProtocolRootActor(ProtocolType.SequenceSliceDistribution).ref());
    }

    public void testSendNextSlicePartOnAcknowledge()
    {
        val control = control();
        control.getModel().setMaximumMessageSizeProvider(() -> (long) SEQUENCE_SIZE * Float.BYTES);
        control.getModel().setSliceSizeFactor(1.0f);
        val messageInterface = messageInterface(control);

        val ack = SequenceSliceDistributionMessages.AcknowledgeSequenceSlicePartMessage.builder()
                                                                                       .sender(localSliceReceiver.ref())
                                                                                       .receiver(self.ref())
                                                                                       .build();
        messageInterface.apply(ack);

        val message = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(message.getPartIndex()).isZero();
        assertThat(message.isLastPart()).isTrue();
        assertThat(message.getSlicePart()).containsExactly(range(0, 100));

        assertThatMessageIsCompleted(ack);
    }

    public void testSendTwoSliceParts()
    {
        val control = control();
        control.getModel().setMaximumMessageSizeProvider(() -> (long) (SEQUENCE_SIZE * 0.5f * Float.BYTES));
        control.getModel().setSliceSizeFactor(1.0f);
        val messageInterface = messageInterface(control);

        control.preStart();
        val firstMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(firstMessage.getPartIndex()).isZero();
        assertThat(firstMessage.isLastPart()).isFalse();
        assertThat(firstMessage.getSlicePart()).containsExactly(range(0, 50));

        val ack = SequenceSliceDistributionMessages.AcknowledgeSequenceSlicePartMessage.builder()
                                                                                       .sender(localSliceReceiver.ref())
                                                                                       .receiver(self.ref())
                                                                                       .build();
        messageInterface.apply(ack);
        val secondMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(secondMessage.getPartIndex()).isOne();
        assertThat(secondMessage.isLastPart()).isTrue();
        assertThat(secondMessage.getSlicePart()).containsExactly(range(50, 50));
        assertThat(secondMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
    }

    public void testOddMessageSize()
    {
        val control = control();
        control.getModel().setMaximumMessageSizeProvider(() -> (long) SEQUENCE_SIZE * Float.BYTES);
        control.getModel().setSliceSizeFactor(0.75f);
        val messageInterface = messageInterface(control);

        control.preStart();
        val firstMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(firstMessage.getPartIndex()).isZero();
        assertThat(firstMessage.isLastPart()).isFalse();
        assertThat(firstMessage.getSlicePart()).containsExactly(range(0, 75));

        val ack = SequenceSliceDistributionMessages.AcknowledgeSequenceSlicePartMessage.builder()
                                                                                       .sender(localSliceReceiver.ref())
                                                                                       .receiver(self.ref())
                                                                                       .build();
        messageInterface.apply(ack);
        val secondMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(secondMessage.getPartIndex()).isOne();
        assertThat(secondMessage.isLastPart()).isTrue();
        assertThat(secondMessage.getSlicePart()).containsExactly(range(75, 25));
        assertThat(secondMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
    }

    public void testDoNotSendEmptySlicePart()
    {
        val control = control();
        control.getModel().setMaximumMessageSizeProvider(() -> (long) SEQUENCE_SIZE * Float.BYTES);
        control.getModel().setSliceSizeFactor(1.0f);
        val messageInterface = messageInterface(control);

        control.preStart();
        val firstMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(firstMessage.getPartIndex()).isZero();
        assertThat(firstMessage.isLastPart()).isTrue();
        assertThat(firstMessage.getSlicePart()).containsExactly(range(0, 100));

        val ack = SequenceSliceDistributionMessages.AcknowledgeSequenceSlicePartMessage.builder()
                                                                                       .sender(localSliceReceiver.ref())
                                                                                       .receiver(self.ref())
                                                                                       .build();
        messageInterface.apply(ack);
        assertThatMessageIsCompleted(ack);
    }
}
