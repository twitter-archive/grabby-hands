/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.grabbyhands;

import java.util.logging.*;
import java.util.Date;
import java.text.SimpleDateFormat;

public class IndentFormatter extends Formatter
{
  private String lineSeparator = System.getProperty("line.separator");
  private boolean showThreadID;
  private boolean showTimestamp;
  private boolean showStartOfWorldTimestamp;
  private String jvmName;

  public IndentFormatter() {
    super();
    LogManager manager = LogManager.getLogManager();
    String classname = this.getClass().getName();
    showThreadID = Boolean.parseBoolean(
      manager.getProperty(classname +".threads"));
    showTimestamp = Boolean.parseBoolean(
      manager.getProperty(classname +".timestamp"));
    showStartOfWorldTimestamp = Boolean.parseBoolean(
      manager.getProperty(classname +".timestamp.startofworld"));
    if (showStartOfWorldTimestamp) {
      showTimestamp = true;
    }
    String propname = classname +".jvmname";
    jvmName = manager.getProperty(propname);
    if (jvmName == null) {
      jvmName = System.getProperty(propname);
    }
    if (jvmName == null) {
      jvmName = "";
    } else {
      jvmName += "."; // addd separator: jvnName.thread#
    }
    startofworldTimestamp = System.currentTimeMillis();
  }
  static private long startofworldTimestamp;

  public String format(LogRecord record) {
    StringBuilder sb = new StringBuilder(256);
    int spaces = (Level.INFO.intValue() - record.getLevel().intValue()) / 100;

    int idx;
    // Prepend fewer spaces at more severe levels
    for (idx = 0; idx <= spaces; idx++) {
      if (idx <= 2) {
        sb.append(" ");
      } else {
        sb.append("    ");
      }
    }
    sb.append(formatMessage(record));
    sb.append(" ");
    for (idx = sb.length(); idx < 100; idx ++) {
      sb.append(" ");
    }
    sb.append(record.getLevel().getLocalizedName());
    if (showThreadID || showTimestamp) {
      sb.append(" [");
      if (showThreadID) {
        sb.append(jvmName);
        sb.append(record.getThreadID());
      }
      if (showTimestamp) {
        if (showThreadID) {
          sb.append(" ");
        }
        long ts = record.getMillis();
        if (showStartOfWorldTimestamp) {
          ts -= startofworldTimestamp;
          sb.append(ts);
        } else {
          sb.append(formatTime(ts));
        }
      }
      sb.append("]");
    }
    sb.append(": ");

    if (record.getSourceClassName() != null) {
      sb.append(record.getSourceClassName());
    } else {
      sb.append(record.getLoggerName());
    }
    if (record.getSourceMethodName() != null) {
      sb.append(" ");
      sb.append(record.getSourceMethodName());
    }
    if (record.getThrown() != null) {
      sb.append(" ");
      sb.append(record.getThrown().getMessage());
    }
    sb.append(lineSeparator);
    return sb.toString();
  }

  // reuse objects to minimize memory allocations
  private Date date = new Date();
  private SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

  protected String formatTime(long millis) {
    date.setTime(millis);
    return dateFormat.format(date);
  }
}
