package de.hpi.msc.jschneider.bootstrap.command.validation;

import com.beust.jcommander.IStringConverter;

import java.nio.file.Path;
import java.nio.file.Paths;

public class StringToPathConverter implements IStringConverter<Path>
{
    @Override
    public Path convert(final String value)
    {
        return Paths.get(value);
    }
}
