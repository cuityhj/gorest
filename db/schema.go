package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	dropPublicSchemaSql = "drop schema if exists public cascade"
	createLxSchemaSql   = "create schema if not exists gr"
)

func InitSchema(pool *pgxpool.Pool) error {
	_, err := pool.Exec(context.TODO(), dropPublicSchemaSql)
	if err != nil {
		return err
	}

	_, err = pool.Exec(context.TODO(), createLxSchemaSql)
	return err
}
