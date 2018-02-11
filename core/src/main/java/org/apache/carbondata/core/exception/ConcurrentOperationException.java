/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.exception;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

/**
 * This exception will be thrown when executing concurrent operations which
 * is not supported in carbon.
 *
 * For example, when INSERT OVERWRITE is executing, other operations are not
 * allowed, so this exception will be thrown
 */
@InterfaceAudience.User
@InterfaceStability.Stable
public class ConcurrentOperationException extends Exception {

  public ConcurrentOperationException(String dbName, String tableName, String command1,
      String command2) {
    super(command1 + " is in progress for table " + dbName + "." + tableName + ", " + command2 +
        " operation is not allowed");
  }

  public ConcurrentOperationException(CarbonTable table, String command1, String command2) {
    this(table.getDatabaseName(), table.getTableName(), command1, command2);
  }

  public String getMessage() {
    return super.getMessage();
  }

}

