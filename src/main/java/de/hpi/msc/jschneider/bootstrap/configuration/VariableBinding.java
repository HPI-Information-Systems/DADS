package de.hpi.msc.jschneider.bootstrap.configuration;

import java.util.Objects;
import java.util.regex.Pattern;

class VariableBinding
{
    private final String _pattern;
    private final String _value;

    VariableBinding(String name, Object value)
    {
        _pattern = Pattern.quote("$" + name);
        _value = Objects.toString(value);
    }

    String apply(String input)
    {
        return input.replaceAll(_pattern, _value);
    }
}
