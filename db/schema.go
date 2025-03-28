package db

import (
	"context"
	"fmt"
)

const (
	dropSchemaSql   = "drop schema if exists %s cascade"
	querySchemaSql  = "select count(1) from information_schema.schemata where schema_name='gr'"
	createSchemaSql = "create schema gr"
)

func InitDBSchema(db DB, dropSchemaList ...string) error {
	for _, schemaName := range dropSchemaList {
		if err := db.Exec(context.TODO(), fmt.Sprintf(dropSchemaSql, schemaName)); err != nil {
			return err
		}
	}

	rows, err := db.Query(context.TODO(), querySchemaSql)
	if err != nil {
		return err
	}

	var count int64
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return err
		}
	}

	if count == 0 {
		return db.Exec(context.TODO(), createSchemaSql)
	}

	return nil
}
