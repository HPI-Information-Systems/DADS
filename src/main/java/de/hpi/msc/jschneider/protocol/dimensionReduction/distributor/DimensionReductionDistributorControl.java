package de.hpi.msc.jschneider.protocol.dimensionReduction.distributor;

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
                                                     (dataDistributor, operationId) -> DimensionReductionMessages.InitializePrincipalComponentsTransferMessage.builder()
                                                                                                                                                              .sender(dataDistributor)
                                                                                                                                                              .receiver(getModel().getReceiverProtocol().getRootActor())
                                                                                                                                                              .operationId(operationId)
                                                                                                                                                              .build());
    }

    private void distributeRotation()
    {
        getModel().getDataTransferManager().transfer(GenericDataSource.create(getModel().getRotation()),
                                                     (dataDistributor, operationId) -> DimensionReductionMessages.InitializeRotationTransferMessage.builder()
                                                                                                                                                   .sender(dataDistributor)
                                                                                                                                                   .receiver(getModel().getReceiverProtocol().getRootActor())
                                                                                                                                                   .operationId(operationId)
                                                                                                                                                   .build());
    }

    private void distributeColumnMeans()
    {
        getModel().getDataTransferManager().transfer(GenericDataSource.create(getModel().getColumnMeans()),
                                                     (dataDistributor, operationId) -> DimensionReductionMessages.InitializeColumnMeansTransferMessage.builder()
                                                                                                                                                      .sender(dataDistributor)
                                                                                                                                                      .receiver(getModel().getReceiverProtocol().getRootActor())
                                                                                                                                                      .operationId(operationId)
                                                                                                                                                      .numberOfColumns(getModel().getColumnMeans().countColumns())
                                                                                                                                                      .build());
    }
}
