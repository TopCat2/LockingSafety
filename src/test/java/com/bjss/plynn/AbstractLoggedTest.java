package com.bjss.plynn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;

import java.lang.reflect.Method;

public abstract class AbstractLoggedTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLoggedTest.class);

    @BeforeMethod
    public void handleTestMethodName(Method method)
    {
        LOGGER.info("Running test method: {}.{}", method.getDeclaringClass().getSimpleName(), method.getName());
    }
}