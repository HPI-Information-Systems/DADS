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
    private final int SEQUENCE_LENGTH = 100;
    private final int SUB_SEQUENCE_LENGTH = 10;

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
                                                          .sequenceReader(new MockSequenceReader(SEQUENCE_LENGTH))
                                                          .maximumMessageSizeProvider(() -> (long) (SEQUENCE_LENGTH / 2))
                                                          .firstSubSequenceIndex(0L)
                                                          .subSequenceLength(SUB_SEQUENCE_LENGTH)
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

    public void testPreStartSendsInitialization()
    {
        val control = control();
        control.preStart();

        val message = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.InitializeSliceTransferMessage.class);
        assertThat(message.getReceiver()).isEqualTo(localProcessor.getProtocolRootActor(ProtocolType.SequenceSliceDistribution).ref());
        assertThat(message.getSubSequenceLength()).isEqualTo(SUB_SEQUENCE_LENGTH);
        assertThat(message.getFirstSubSequenceIndex()).isEqualTo(0L);
    }

    public void testSendEntireSliceOnRequest()
    {
        val control = control();
        control.getModel().setMaximumMessageSizeProvider(() -> (long) SEQUENCE_LENGTH * Float.BYTES);
        control.getModel().setSliceSizeFactor(1.0f);
        val messageInterface = messageInterface(control);

        val request = SequenceSliceDistributionMessages.RequestNextSlicePartMessage.builder()
                                                                                   .sender(localSliceReceiver.ref())
                                                                                   .receiver(self.ref())
                                                                                   .build();
        messageInterface.apply(request);

        val slicePartMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(slicePartMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
        assertThat(slicePartMessage.isLastPart()).isTrue();
        assertThat(slicePartMessage.getSlicePart()).containsExactly(range(0, SEQUENCE_LENGTH));
        assertThatMessageIsCompleted(request);
    }

    public void testSendSlicePartOnRequest()
    {
        val control = control();
        control.getModel().setMaximumMessageSizeProvider(() -> (long) SEQUENCE_LENGTH * Float.BYTES);
        control.getModel().setSliceSizeFactor(0.5f);
        val messageInterface = messageInterface(control);
        var sequenceLengthHalf = (int)(SEQUENCE_LENGTH * 0.5f);

        val request = SequenceSliceDistributionMessages.RequestNextSlicePartMessage.builder()
                                                                                   .sender(localSliceReceiver.ref())
                                                                                   .receiver(self.ref())
                                                                                   .build();
        messageInterface.apply(request);

        val firstPartMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(firstPartMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
        assertThat(firstPartMessage.isLastPart()).isFalse();
        assertThat(firstPartMessage.getSlicePart()).containsExactly(range(0, sequenceLengthHalf));
        assertThatMessageIsCompleted(request);

        messageInterface.apply(request);

        val secondPartMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(secondPartMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
        assertThat(secondPartMessage.isLastPart()).isTrue();
        assertThat(secondPartMessage.getSlicePart()).containsExactly(range(sequenceLengthHalf, sequenceLengthHalf));
        assertThatMessageIsCompleted(request);
    }

    public void testDoNotSendEmptySlicePart()
    {
        val control = control();
        control.getModel().setMaximumMessageSizeProvider(() -> (long) SEQUENCE_LENGTH * Float.BYTES);
        control.getModel().setSliceSizeFactor(1.0f);
        val messageInterface = messageInterface(control);

        val request = SequenceSliceDistributionMessages.RequestNextSlicePartMessage.builder()
                                                                                   .sender(localSliceReceiver.ref())
                                                                                   .receiver(self.ref())
                                                                                   .build();
        messageInterface.apply(request);

        val slicePartMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class);
        assertThat(slicePartMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
        assertThat(slicePartMessage.isLastPart()).isTrue();
        assertThat(slicePartMessage.getSlicePart()).containsExactly(range(0, SEQUENCE_LENGTH));
        assertThatMessageIsCompleted(request);

        messageInterface.apply(request);
        assertThatMessageIsCompleted(request);
    }
}
