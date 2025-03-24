package db

import (
	"context"
	"fmt"
)

const (
	dropSchemaSql   = "drop schema if exists %s cascade"
	createSchemaSql = "create schema if not exists gr"
)

func InitDBSchema(db DB, dropSchemaList ...string) error {
	for _, schemaName := range dropSchemaList {
		if err := db.Exec(context.TODO(), fmt.Sprintf(dropSchemaSql, schemaName)); err != nil {
			return err
		}
	}

	return db.Exec(context.TODO(), createSchemaSql)
}
