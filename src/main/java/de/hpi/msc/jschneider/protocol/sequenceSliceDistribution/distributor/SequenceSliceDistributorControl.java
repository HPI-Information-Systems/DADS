package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
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
            getLog().error("Unable to initialize slice part transfer to {}, because the receiver does not provide the SequenceSliceDistribution protocol!",
                           getModel().getSliceReceiverActorSystem());
            return;
        }

        getModel().getDataTransferManager().transfer(getModel().getSequenceReader(), this::initializationMessageFactory);

        getLog().debug("Starting sequence slice transfer to {}.", getModel().getSliceReceiverActorSystem());
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

    @Override
    protected void onDataTransferFinished(long operationId)
    {
        getModel().getSelf().tell(PoisonPill.getInstance(), getModel().getSelf());
    }
}
