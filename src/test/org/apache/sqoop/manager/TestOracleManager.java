/**
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

package org.apache.sqoop.manager;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.oracle.util.OracleUtils;
import org.junit.Test;

public class TestOracleManager {
  public static final Log LOG = LogFactory.getLog(TestOracleManager.class.getName());
  static final String TABLE_NAME = "EMPLOYEES";

  private OracleManager getOracleManager() {
    SqoopOptions options = new SqoopOptions(OracleUtils.CONNECT_STRING, TABLE_NAME);
    OracleUtils.setOracleAuth(options);

    OracleManager manager = new OracleManager(options);
    return manager;
  }

  private OracleManager getSpiedOracleManager() {
    OracleManager manager = spy(getOracleManager());
    return manager;
  }

  @Test(expected = SQLException.class)
  public void testExceptionThrownOnFailureWhilePreparingConnection() throws Exception {
    OracleManager manager = getSpiedOracleManager();
    doThrow(new SQLException()).when(manager).prepareConnection(any(Connection.class));

    manager.getConnection();
  }

  @Test
  public void testConnectionClosedOnFailureWhilePreparingConnection() throws Exception {
    OracleManager manager = getOracleManager();
    Connection connMock = spy(manager.getConnection());

    OracleManager managerSpy = getSpiedOracleManager();
    doThrow(new SQLException()).when(managerSpy).prepareConnection(connMock);

    try {
      managerSpy.getConnection();
    } catch (SQLException e) {
      LOG.info(String.format("SQLException thrown: ", e.getMessage()));
      verify(connMock, times(1)).close();
    }

    manager.close();
    managerSpy.close();
  }
}
