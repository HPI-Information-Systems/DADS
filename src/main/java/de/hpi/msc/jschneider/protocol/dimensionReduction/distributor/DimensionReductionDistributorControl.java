package de.hpi.msc.jschneider.protocol.dimensionReduction.distributor;

import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;

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

        distributeRotation();
        distributePrincipalComponents();
        distributeColumnMeans();
    }

    private void distributePrincipalComponents()
    {
        getModel().getDataTransferManager().transfer(DataSource.create(getModel().getPrincipalComponents()),
                                                     (dataDistributor, operationId) ->
                                                     {
                                                         getModel().setPrincipalComponentsTransferOperationId(operationId);
                                                         return DimensionReductionMessages.InitializePrincipalComponentsTransferMessage.builder()
                                                                                                                                       .sender(dataDistributor)
                                                                                                                                       .receiver(getModel().getReceiverProtocol().getRootActor())
                                                                                                                                       .operationId(operationId)
                                                                                                                                       .build();
                                                     });
    }

    private void distributeRotation()
    {
        getModel().getDataTransferManager().transfer(DataSource.create(getModel().getRotation()),
                                                     (dataDistributor, operationId) ->
                                                     {
                                                         getModel().setRotationTransferOperationId(operationId);
                                                         return DimensionReductionMessages.InitializeRotationTransferMessage.builder()
                                                                                                                            .sender(dataDistributor)
                                                                                                                            .receiver(getModel().getReceiverProtocol().getRootActor())
                                                                                                                            .operationId(operationId)
                                                                                                                            .build();
                                                     });
    }

    private void distributeColumnMeans()
    {
        getModel().getDataTransferManager().transfer(DataSource.create(getModel().getColumnMeans()),
                                                     (dataDistributor, operationId) ->
                                                     {
                                                         getModel().setColumnMeansTransferOperationId(operationId);
                                                         return DimensionReductionMessages.InitializeColumnMeansTransferMessage.builder()
                                                                                                                               .sender(dataDistributor)
                                                                                                                               .receiver(getModel().getReceiverProtocol().getRootActor())
                                                                                                                               .operationId(operationId)
                                                                                                                               .numberOfColumns(getModel().getColumnMeans().countColumns())
                                                                                                                               .build();
                                                     });
    }

    @Override
    protected void onDataTransferFinished(long operationId)
    {
        super.onDataTransferFinished(operationId);

        getModel().setPrincipalComponentsTransferCompleted(getModel().isPrincipalComponentsTransferCompleted() || operationId == getModel().getPrincipalComponentsTransferOperationId());
        getModel().setRotationTransferCompleted(getModel().isRotationTransferCompleted() || operationId == getModel().getRotationTransferOperationId());
        getModel().setColumnMeansTransferCompleted(getModel().isColumnMeansTransferCompleted() || operationId == getModel().getColumnMeansTransferOperationId());

        if (getModel().isPrincipalComponentsTransferCompleted() &&
            getModel().isRotationTransferCompleted() &&
            getModel().isColumnMeansTransferCompleted())
        {
            isReadyToBeTerminated();
        }
    }
}
