//go:build iceberg_glue

package iceberg

import _ "github.com/apache/iceberg-go/catalog/glue" // AWS Glue Data Catalog (native API)

func init() { catalogTypes["glue"] = true }
