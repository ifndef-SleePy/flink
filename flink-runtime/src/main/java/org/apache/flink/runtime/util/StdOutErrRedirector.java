/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * Redirect std out and std err to logger.
 */
public class StdOutErrRedirector {

	public static final String STDOUT_FILE_PROPERTY_KEY = "stdout.file";

	public static final String STDERR_FILE_PROPERTY_KEY = "stderr.file";

	private static final String STDOUT_LOGGER_NAME = "StdOutErrRedirector.Stdout";

	private static final String STDERR_LOGGER_NAME = "StdOutErrRedirector.Stderr";

	private static final Logger stdoutLogger = LoggerFactory.getLogger(STDOUT_LOGGER_NAME);

	private static final Logger stderrLogger = LoggerFactory.getLogger(STDERR_LOGGER_NAME);

	private static final ThreadLocal<Boolean> isRedirecting = ThreadLocal.withInitial(() -> false);

	/**
	 * Try to redirect stdout and stderr.
	 */
	public static void redirectStdOutErr() {

		if (System.getProperty(STDOUT_FILE_PROPERTY_KEY) != null) {
			System.setOut(createLoggerProxy(stdoutLogger, System.out));
		}

		if (System.getProperty(STDERR_FILE_PROPERTY_KEY) != null) {
			System.setErr(createLoggerProxy(stderrLogger, System.err));
		}
	}

	/**
	 * Create logger proxy print stream.
	 * Do not check null, keep the same behavior with System.out/err.
	 *
	 * @param logger              the logger
	 * @param originalPrintStream the original print stream
	 * @return the proxy print stream
	 */
	@VisibleForTesting
	static PrintStream createLoggerProxy(final Logger logger, final PrintStream originalPrintStream) {
		return new PrintStream(originalPrintStream) {

			@Override
			public void print(boolean b) {
				print(b ? "true" : "false");
			}

			@Override
			public void print(char c) {
				print(String.valueOf(c));
			}

			@Override
			public void print(int i) {
				print(String.valueOf(i));
			}

			@Override
			public void print(long l) {
				print(String.valueOf(l));
			}

			@Override
			public void print(float f) {
				print(String.valueOf(f));
			}

			@Override
			public void print(double d) {
				print(String.valueOf(d));
			}

			@Override
			public void print(char[] s) {
				print(new String(s));
			}

			@Override
			public void print(String s) {
				// if it is already redirecting, use original print stream to prevent recursive call
				if (!isRedirecting.get()) {
					try {
						isRedirecting.set(true);
						logger.info(s);
					} finally {
						isRedirecting.set(false);
					}
				} else {
					originalPrintStream.print(s);
				}
			}

			@Override
			public void print(Object obj) {
				print(String.valueOf(obj));
			}

			void newLine() {
				print(System.lineSeparator());
			}

			@Override
			public void println() {
				newLine();
			}

			@Override
			public void println(boolean b) {
				print(b);
				newLine();
			}

			@Override
			public void println(char c) {
				print(c);
				newLine();
			}

			@Override
			public void println(int i) {
				print(i);
				newLine();
			}

			@Override
			public void println(long l) {
				print(l);
				newLine();
			}

			@Override
			public void println(float f) {
				print(f);
				newLine();
			}

			@Override
			public void println(double d) {
				print(d);
				newLine();
			}

			@Override
			public void println(char[] s) {
				print(s);
				newLine();
			}

			@Override
			public void println(String s) {
				print(s);
				newLine();
			}

			@Override
			public void println(Object obj) {
				print(obj);
				newLine();
			}

			@Override
			public PrintStream append(CharSequence csq, int start, int end) {
				CharSequence cs = (csq == null ? "null" : csq);
				print(cs.subSequence(start, end).toString());
				return this;
			}
		};
	}

	@VisibleForTesting
	static boolean isRedirecting() {
		return isRedirecting.get();
	}
}
