/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.aws.glue;

import java.util.List;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableOperations;

public class GlueTable extends BaseTable {

  public GlueTable(TableOperations ops, String name) {
    super(ops, name);
  }

  /**
   * Get the authorized columns configured by the table.
   *
   * @return authorized columns of the Glue table, or null if not configured
   */
  public List<String> authorizedColumns() {
    return ((GlueTableOperations) operations()).authorizedColumns();
  }
}
