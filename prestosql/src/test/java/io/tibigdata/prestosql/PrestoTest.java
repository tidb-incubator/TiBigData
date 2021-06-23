package io.tibigdata.prestosql;

import static io.prestosql.testing.MaterializedResult.DEFAULT_PRECISION;

import com.google.common.collect.ImmutableList;
import io.prestosql.testing.MaterializedRow;
import java.util.List;
import org.junit.Test;

public class PrestoTest {

  private final TiDBQueryRunner tiDBQueryRunner = new TiDBQueryRunner();

  public void preCommand() {
    tiDBQueryRunner.execute("CREATE TABLE sample_table(c1 int,c2 varchar)");
    tiDBQueryRunner.execute("INSERT INTO sample_table values(1,'zs')");
    tiDBQueryRunner.execute("INSERT INTO sample_table values(2,'ls')");
  }

  public void afterCommand() {
    tiDBQueryRunner.execute("DROP TABLE sample_table");
  }

  @Test
  public void test() {
    preCommand();
    try {
      String sql = "SELECT * FROM sample_table";
      List<MaterializedRow> targetRows = ImmutableList
          .of(new MaterializedRow(DEFAULT_PRECISION, 1, "zs"),
              new MaterializedRow(DEFAULT_PRECISION, 2, "ls"));
      tiDBQueryRunner.verifySqlResult(sql, targetRows);

      sql = "SELECT c1,c2 FROM sample_table WHERE c1 = 1";
      targetRows = ImmutableList
          .of(new MaterializedRow(DEFAULT_PRECISION, 1, "zs"));
      tiDBQueryRunner.verifySqlResult(sql, targetRows);

      sql = "SELECT * FROM sample_table WHERE c1 = 1 OR c1 = 2";
      targetRows = ImmutableList
          .of(new MaterializedRow(DEFAULT_PRECISION, 1, "zs"),
              new MaterializedRow(DEFAULT_PRECISION, 2, "ls"));
      tiDBQueryRunner.verifySqlResult(sql, targetRows);
    } catch (Exception e) {
      throw e;
    } finally {
      afterCommand();
    }
  }

}
