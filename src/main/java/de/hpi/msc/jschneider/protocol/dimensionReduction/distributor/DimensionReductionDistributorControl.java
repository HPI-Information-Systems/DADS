package de.hpi.msc.jschneider.protocol.dimensionReduction.distributor;

import akka.actor.PoisonPill;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;

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
        getModel().getDataTransferManager().transfer(GenericDataSource.create(getModel().getPrincipalComponents()),
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
        getModel().getDataTransferManager().transfer(GenericDataSource.create(getModel().getRotation()),
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
        getModel().getDataTransferManager().transfer(GenericDataSource.create(getModel().getColumnMeans()),
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
        getModel().setPrincipalComponentsTransferCompleted(getModel().isPrincipalComponentsTransferCompleted() || operationId == getModel().getPrincipalComponentsTransferOperationId());
        getModel().setRotationTransferCompleted(getModel().isRotationTransferCompleted() || operationId == getModel().getRotationTransferOperationId());
        getModel().setColumnMeansTransferCompleted(getModel().isColumnMeansTransferCompleted() || operationId == getModel().getColumnMeansTransferOperationId());

        if (getModel().isPrincipalComponentsTransferCompleted() &&
            getModel().isRotationTransferCompleted() &&
            getModel().isColumnMeansTransferCompleted())
        {
            getModel().getSelf().tell(PoisonPill.getInstance(), getModel().getSelf());
        }
    }
}
