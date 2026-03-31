package liteq

import "embed"

//go:embed schema/*.sql
var schemaFS embed.FS
