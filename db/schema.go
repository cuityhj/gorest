package db

const (
	dropSchemaSql              = "drop schema if exists %s cascade"
	querySchemaSql             = "select count(1) from information_schema.schemata where schema_name='gr'"
	createSchemaSql            = "create schema gr"
	createSchemaIfNotExistsSql = "create schema if not exists gr"
)
