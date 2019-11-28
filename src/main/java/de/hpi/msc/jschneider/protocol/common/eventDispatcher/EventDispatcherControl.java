package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public interface EventDispatcherControl<TModel extends EventDispatcherModel>
{
    TModel getModel();

    void setModel(TModel model);

    ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder);
}
