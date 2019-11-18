package de.hpi.msc.jschneider.bootstrap.configuration;

import java.util.Objects;

class VariableBinding
{
    private final String _pattern;
    private final String _value;

    VariableBinding(final String pattern, final Object value)
    {
        _pattern = pattern;
        _value = Objects.toString(value);
    }

    String apply(final String input)
    {
        return input.replaceAll(_pattern, _value);
    }
}
