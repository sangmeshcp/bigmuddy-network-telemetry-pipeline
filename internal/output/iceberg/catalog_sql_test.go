package iceberg

// Tests use the SQL catalog (SQLite) whatever the build tags.
import _ "github.com/apache/iceberg-go/catalog/sql"

func init() { catalogTypes["sql"] = true }
