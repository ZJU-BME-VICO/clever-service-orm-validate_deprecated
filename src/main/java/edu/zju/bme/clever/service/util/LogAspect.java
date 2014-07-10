package edu.zju.bme.clever.service.util;

import org.apache.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;

@Component
@Aspect
public class LogAspect {

	private final Logger logger = Logger.getLogger(LogAspect.class.getName());

	@Before(value = "execution(public * edu.zju.bme.clever.service..*.*(..))")
	public void before(JoinPoint jp) {
		String className = jp.getTarget().getClass().getName();
		String methodName = jp.getSignature().getName();
		logger.info(className + "::" + methodName);
	}

	@After(value = "execution(public * edu.zju.bme.clever.service..*.*(..))")
	public void after(JoinPoint jp) {
	}

}
