package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataDistributor;
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

        getModel().getDataTransferManager().transfer(getModel().getSequenceReader(), this::initializeSequenceDistributor, this::initializationMessageFactory);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder);
    }

    private DataDistributor initializeSequenceDistributor(DataDistributor distributor)
    {
        return distributor.whenFinished(this::whenFinished);
    }

    private void whenFinished(DataDistributor distributor)
    {
        // TODO: terminate self?!
    }

    private SequenceSliceDistributionMessages.InitializeSequenceSliceTransferMessage initializationMessageFactory(DataDistributor distributor)
    {
        val protocol = getProtocol(getModel().getSliceReceiverActorSystem(), ProtocolType.SequenceSliceDistribution).get(); // no need to check, because we already checked that in preStart

        return SequenceSliceDistributionMessages.InitializeSequenceSliceTransferMessage.builder()
                .sender(getModel().getSelf())
                .receiver(protocol.getRootActor())
                .subSequenceLength(getModel().getSubSequenceLength())
                .convolutionSize(getModel().getConvolutionSize())
                .firstSubSequenceIndex(getModel().getFirstSubSequenceIndex())
                .build();
    }
}
