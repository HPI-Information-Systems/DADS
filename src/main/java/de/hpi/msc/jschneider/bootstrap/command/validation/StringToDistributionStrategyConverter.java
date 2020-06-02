package de.hpi.msc.jschneider.bootstrap.command.validation;

import com.beust.jcommander.IStringConverter;
import de.hpi.msc.jschneider.bootstrap.command.DistributionStrategy;

public class StringToDistributionStrategyConverter implements IStringConverter<DistributionStrategy>
{
    @Override
    public DistributionStrategy convert(String s)
    {
        return DistributionStrategy.fromString(s);
    }
}
