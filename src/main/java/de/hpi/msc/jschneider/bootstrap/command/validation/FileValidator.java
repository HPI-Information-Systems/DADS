package de.hpi.msc.jschneider.bootstrap.command.validation;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import lombok.SneakyThrows;
import lombok.val;

import java.nio.file.Path;

public class FileValidator implements IValueValidator<Path>
{
    @SneakyThrows
    @Override
    public void validate(String parameterName, Path parameterValue) throws ParameterException
    {
        val file = parameterValue.toFile();
        if (!file.exists() && !file.createNewFile())
        {
            throw new ParameterException(String.format("\"%1$s\" can not be found/created!", parameterValue));
        }

        if (!file.isFile())
        {
            throw new ParameterException(String.format("%1$s must specify a file!", parameterName));
        }
    }
}
