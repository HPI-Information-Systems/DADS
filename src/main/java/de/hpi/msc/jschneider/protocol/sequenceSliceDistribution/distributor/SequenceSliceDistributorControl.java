package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

public class SequenceSliceDistributorControl extends AbstractProtocolParticipantControl<SequenceSliceDistributorModel>
{
    public SequenceSliceDistributorControl(SequenceSliceDistributorModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        val protocol = getProtocol(getModel().getSliceReceiverActorSystem(), ProtocolType.SequenceSliceDistribution);
        if (!protocol.isPresent())
        {
            getLog().error(String.format("Unable to initialize slice part transfer to %1$s, because the receiver does not provide the SequenceSliceDistribution protocol!",
                                         getModel().getSliceReceiverActorSystem()));
            return;
        }

        getModel().getDataTransferManager().transfer(getModel().getSequenceReader(), this::initializationMessageFactory);

        getLog().info(String.format("Starting sequence slice transfer to %1$s.", getModel().getSliceReceiverActorSystem()));
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder);
    }

    private SequenceSliceDistributionMessages.InitializeSequenceSliceTransferMessage initializationMessageFactory(ActorRef dataDistributor, long operationId)
    {
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        val protocol = getProtocol(getModel().getSliceReceiverActorSystem(), ProtocolType.SequenceSliceDistribution).get(); // no need to check, because we already checked that in preStart

        return SequenceSliceDistributionMessages.InitializeSequenceSliceTransferMessage.builder()
                                                                                       .sender(dataDistributor)
                                                                                       .receiver(protocol.getRootActor())
                                                                                       .subSequenceLength(getModel().getSubSequenceLength())
                                                                                       .convolutionSize(getModel().getConvolutionSize())
                                                                                       .firstSubSequenceIndex(getModel().getFirstSubSequenceIndex())
                                                                                       .isLastSubSequenceChunk(getModel().isLastSubSequenceChunk())
                                                                                       .operationId(operationId)
                                                                                       .build();
    }
}
