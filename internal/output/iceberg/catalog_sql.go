//go:build iceberg_sql

package iceberg

// SQL catalog (Postgres, MySQL, SQLite, ...). Requires a database/sql
// driver registered in the build as well.
import _ "github.com/apache/iceberg-go/catalog/sql"

func init() { catalogTypes["sql"] = true }
