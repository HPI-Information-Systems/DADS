package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.rootActor;

import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAMessages;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator.PCACalculatorControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator.PCACalculatorModel;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.coordinator.PCACoordinatorControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.coordinator.PCACoordinatorModel;
import de.hpi.msc.jschneider.protocol.reaper.ReapedActor;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

public class PCARootActorControl extends AbstractProtocolParticipantControl<PCARootActorModel>
{
    public PCARootActorControl(PCARootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(PCAMessages.InitializePCACalculationMessage.class, message -> forward(message, getModel().getCalculator()))
                    .match(PCAMessages.InitializeColumnMeansTransferMessage.class, message -> forward(message, getModel().getCalculator()))
                    .match(PCAMessages.InitializeRTransferMessage.class, message -> forward(message, getModel().getCalculator()));
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        createPCACalculator();
        createPCACoordinator();
    }

    private void createPCACalculator()
    {
        val model = PCACalculatorModel.builder()
                                      .build();
        val control = new PCACalculatorControl(model);
        val calculator = trySpawnChild(ReapedActor.props(control), "PCACalculator");
        if (!calculator.isPresent())
        {
            getLog().error("Unable to create PCACalculator!");
            return;
        }

        getModel().setCalculator(calculator.get());
    }

    private void createPCACoordinator()
    {
        if (!getModel().getLocalProcessor().isMaster())
        {
            return;
        }

        val command = (MasterCommand) SystemParameters.getCommand();
        val numberOfParticipants = Math.max(command.getMinimumNumberOfSlaves(), getModel().getNumberOfProcessors());

        val model = PCACoordinatorModel.builder()
                                       .numberOfParticipants(numberOfParticipants)
                                       .build();
        val control = new PCACoordinatorControl(model);
        val coordinator = trySpawnChild(ReapedActor.props(control), "PCACoordinator");
        if (!coordinator.isPresent())
        {
            getLog().error("Unable to create PCACoordinator!");
            return;
        }

        getModel().setCoordinator(coordinator.get());
    }
}