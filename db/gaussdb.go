package db

import (
	"context"
	"fmt"

	"github.com/cuityhj/pgx/v5/pgxpool"
)

//pgx has adapted gaussdb
type GaussDB struct {
	pool   *pgxpool.Pool
	driver Driver
}

func NewGaussDB(connStr string) (DB, error) {
	if pool, err := pgxpool.New(context.TODO(), connStr); err != nil {
		return nil, err
	} else {
		return &GaussDB{pool: pool, driver: DriverOpenGauss}, nil
	}
}

func (gs *GaussDB) IsRecoveryMode() (bool, error) {
	return DBIsRecoveryMode(gs)
}

func (gs *GaussDB) InitSchema(dropSchemaList ...string) error {
	for _, schemaName := range dropSchemaList {
		if err := gs.Exec(context.TODO(), fmt.Sprintf(dropSchemaSql, schemaName)); err != nil {
			return err
		}
	}

	rows, err := gs.Query(context.TODO(), querySchemaSql)
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
		return gs.Exec(context.TODO(), createSchemaSql)
	}

	return nil
}

func (gs *GaussDB) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := gs.pool.Exec(ctx, sql, args...)
	return err
}

func (gs *GaussDB) Query(ctx context.Context, sql string, args ...any) (DBRows, error) {
	return gs.pool.Query(ctx, sql, args...)
}

//return PGTx without rewrite other interfaces
func (gs *GaussDB) Begin() (Tx, error) {
	if tx, err := gs.pool.Begin(context.TODO()); err != nil {
		return nil, err
	} else {
		return &PGTx{tx: tx, driver: gs.driver}, nil
	}
}

func (gs *GaussDB) Close() error {
	gs.pool.Close()
	return nil
}

func (gs *GaussDB) GetDriver() Driver {
	return gs.driver
}
