package de.hpi.msc.jschneider.protocol.dimensionReduction.distributor;

import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferManager;

public class DimensionReductionDistributorControl extends AbstractProtocolParticipantControl<DimensionReductionDistributorModel>
{
    public DimensionReductionDistributorControl(DimensionReductionDistributorModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        getModel().getDataTransferManager().whenAllTransfersFinished(this::onAllTransfersFinished);

        distributeRotation();
        distributePrincipalComponents();
        distributeColumnMeans();
    }

    private void distributePrincipalComponents()
    {
        getModel().getDataTransferManager().transfer(getModel().getPrincipalComponents(), dataDistributor ->
                DimensionReductionMessages.InitializePrincipalComponentsTransferMessage.builder()
                                                                                       .sender(getModel().getSelf())
                                                                                       .receiver(getModel().getReceiverProtocol().getRootActor())
                                                                                       .operationId(dataDistributor.getOperationId())
                                                                                       .build());
    }

    private void distributeRotation()
    {
        getModel().getDataTransferManager().transfer(getModel().getRotation(), dataDistributor ->
                DimensionReductionMessages.InitializeRotationTransferMessage.builder()
                                                                            .sender(getModel().getSelf())
                                                                            .receiver(getModel().getReceiverProtocol().getRootActor())
                                                                            .operationId(dataDistributor.getOperationId())
                                                                            .build());
    }

    private void distributeColumnMeans()
    {
        getModel().getDataTransferManager().transfer(getModel().getColumnMeans(), dataDistributor ->
                DimensionReductionMessages.InitializeColumnMeansTransferMessage.builder()
                                                                               .sender(getModel().getSelf())
                                                                               .receiver(getModel().getReceiverProtocol().getRootActor())
                                                                               .operationId(dataDistributor.getOperationId())
                                                                               .numberOfColumns(getModel().getColumnMeans().countColumns())
                                                                               .build());
    }

    private void onAllTransfersFinished(DataTransferManager dataTransferManager)
    {
    }
}
