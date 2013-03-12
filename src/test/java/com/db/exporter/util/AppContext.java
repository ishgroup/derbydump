package com.db.exporter.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class AppContext {
	private static ApplicationContext context;

	public static void destroyContext() {

		((ClassPathXmlApplicationContext) context).close();
		context = null;
	}

	public static ApplicationContext getContext() {

		if (context == null) {
			context = new ClassPathXmlApplicationContext(
					new String[] { "resources/test-applicationContext.xml" });

		}
		return context;
	}

	public static void main(String[] args) {
		getContext();
	}

}
