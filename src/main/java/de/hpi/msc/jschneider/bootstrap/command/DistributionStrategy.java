package de.hpi.msc.jschneider.bootstrap.command;

import lombok.val;

public enum DistributionStrategy
{
    HOMOGENEOUS,
    HETEROGENEOUS;

    public static DistributionStrategy fromString(String code)
    {
        for (val value : DistributionStrategy.values())
        {
            if (value.toString().equalsIgnoreCase(code)) {
                return value;
            }
        }

        return HETEROGENEOUS;
    }
}
