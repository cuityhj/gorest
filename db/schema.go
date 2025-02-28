package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	dropSchemaSql   = "drop schema if exists $1 cascade"
	createSchemaSql = "create schema if not exists gr"
)

func InitSchema(pool *pgxpool.Pool, dropSchemaList ...string) error {
	for _, schemaName := range dropSchemaList {
		if _, err := pool.Exec(context.TODO(), dropSchemaSql, schemaName); err != nil {
			return err
		}
	}

	_, err := pool.Exec(context.TODO(), createSchemaSql)
	return err
}
