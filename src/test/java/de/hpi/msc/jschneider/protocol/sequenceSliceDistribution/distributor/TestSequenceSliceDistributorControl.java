package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.fileHandling.MockSequenceReader;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataDistributor;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;
import lombok.var;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSequenceSliceDistributorControl extends ProtocolTestCase
{
    private final int SEQUENCE_LENGTH = 100;
    private final int CONVOLUTION_SIZE = 33;
    private final int SUB_SEQUENCE_LENGTH = 10;
    private final String OPERATION_ID = UUID.randomUUID().toString();

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
                                                          .sliceReceiverActorSystem(localProcessor.getId())
                                                          .sequenceReader(new MockSequenceReader(SEQUENCE_LENGTH))
                                                          .maximumMessageSizeProvider(() -> (long) (SEQUENCE_LENGTH / 2))
                                                          .firstSubSequenceIndex(0L)
                                                          .subSequenceLength(SUB_SEQUENCE_LENGTH)
                                                          .convolutionSize(CONVOLUTION_SIZE)
                                                          .isLastSubSequenceChunk(true)
                                                          .build());
    }

    private SequenceSliceDistributorControl control()
    {
        return new SequenceSliceDistributorControl(dummyModel());
    }

    private SequenceSliceDistributorControl initializedControl()
    {
        val control = control();
        val dataDistributor = new DataDistributor(control, control.getModel().getSequenceReader());

        control.getModel().getDataTransferManager().getDataDistributors().put(OPERATION_ID, dataDistributor);

        return control;
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

        assertThat(control.getModel().getDataTransferManager().getDataDistributors().size()).isOne();

        val message = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(SequenceSliceDistributionMessages.InitializeSequenceSliceTransferMessage.class);
        assertThat(message.getReceiver()).isEqualTo(localProcessor.getProtocolRootActor(ProtocolType.SequenceSliceDistribution).ref());
        assertThat(message.getSubSequenceLength()).isEqualTo(SUB_SEQUENCE_LENGTH);
        assertThat(message.getConvolutionSize()).isEqualTo(CONVOLUTION_SIZE);
        assertThat(message.getFirstSubSequenceIndex()).isEqualTo(0L);
        assertThat(message.isLastSubSequenceChunk()).isTrue();
    }

    public void testSendEntireSliceOnRequest()
    {
        val control = initializedControl();
        control.getModel().setMaximumMessageSizeProvider(() -> Long.MAX_VALUE);
        val messageInterface = createMessageInterface(control);

        val request = DataTransferMessages.RequestNextDataPartMessage.builder()
                                                                     .sender(localSliceReceiver.ref())
                                                                     .receiver(self.ref())
                                                                     .operationId(OPERATION_ID)
                                                                     .build();
        messageInterface.apply(request);

        val slicePartMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.DataPartMessage.class);
        assertThat(slicePartMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
        assertThat(slicePartMessage.isLastPart()).isTrue();
        assertThat(Serialize.toFloats(slicePartMessage.getPart())).containsExactly(range(0, SEQUENCE_LENGTH));

        assertThatMessageIsCompleted(request);
    }

    public void testSendSlicePartOnRequest()
    {
        val control = initializedControl();
        control.getModel().setMaximumMessageSizeProvider(() -> (long) SEQUENCE_LENGTH * Float.BYTES);
        val messageInterface = createMessageInterface(control);

        val request = DataTransferMessages.RequestNextDataPartMessage.builder()
                                                                     .sender(localSliceReceiver.ref())
                                                                     .receiver(self.ref())
                                                                     .operationId(OPERATION_ID)
                                                                     .build();
        messageInterface.apply(request);

        val firstPartMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.DataPartMessage.class);
        assertThat(firstPartMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
        assertThat(firstPartMessage.isLastPart()).isFalse();
        assertThat(Serialize.toFloats(firstPartMessage.getPart())).containsExactly(range(0, (int) (SEQUENCE_LENGTH * DataDistributor.MESSAGE_SIZE_FACTOR)));
        assertThatMessageIsCompleted(request);

        messageInterface.apply(request);

        val secondPartMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.DataPartMessage.class);
        assertThat(secondPartMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
        assertThat(secondPartMessage.isLastPart()).isTrue();
        assertThat(Serialize.toFloats(secondPartMessage.getPart())).containsExactly(range((int) (SEQUENCE_LENGTH * DataDistributor.MESSAGE_SIZE_FACTOR), (int) (SEQUENCE_LENGTH * (1.0f - DataDistributor.MESSAGE_SIZE_FACTOR))));
        assertThatMessageIsCompleted(request);
    }

    public void testDoNotSendEmptySlicePart()
    {
        val control = initializedControl();
        control.getModel().setMaximumMessageSizeProvider(() -> Long.MAX_VALUE);
        val messageInterface = createMessageInterface(control);

        val request = DataTransferMessages.RequestNextDataPartMessage.builder()
                                                                     .sender(localSliceReceiver.ref())
                                                                     .receiver(self.ref())
                                                                     .operationId(OPERATION_ID)
                                                                     .build();
        messageInterface.apply(request);

        val slicePartMessage = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.DataPartMessage.class);
        assertThat(slicePartMessage.getReceiver()).isEqualTo(localSliceReceiver.ref());
        assertThat(slicePartMessage.isLastPart()).isTrue();
        assertThat(Serialize.toFloats(slicePartMessage.getPart())).containsExactly(range(0, SEQUENCE_LENGTH));

        assertThatMessageIsCompleted(request);

        messageInterface.apply(request);
        assertThatMessageIsCompleted(request);
    }
}
