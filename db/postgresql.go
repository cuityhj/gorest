package db

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGDB struct {
	pool *pgxpool.Pool
}

type PGTx struct {
	tx pgx.Tx
}

type PGTxRows struct {
	pgx.Rows
}

func NewPGDB(connStr string) (DB, error) {
	if pool, err := pgxpool.New(context.TODO(), connStr); err != nil {
		return nil, err
	} else {
		return &PGDB{pool}, nil
	}
}

func (pg *PGDB) IsRecoveryMode() (bool, error) {
	return DBIsRecoveryMode(pg)
}

func (pg *PGDB) InitSchema(dropSchemaList ...string) error {
	return InitDBSchema(pg, dropSchemaList...)
}

func (pg *PGDB) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := pg.pool.Exec(ctx, sql, args...)
	return err
}

func (pg *PGDB) Query(ctx context.Context, sql string, args ...any) (DBRows, error) {
	return pg.pool.Query(ctx, sql, args...)
}

func (pg *PGDB) Begin() (Tx, error) {
	if tx, err := pg.pool.Begin(context.TODO()); err != nil {
		return nil, err
	} else {
		return &PGTx{tx}, nil
	}
}

func (pg *PGDB) Close() error {
	pg.pool.Close()
	return nil
}

func (tx *PGTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	if result, err := tx.tx.Exec(ctx, sql, args...); err != nil {
		return 0, err
	} else {
		return result.RowsAffected(), nil
	}
}

func (tx *PGTx) Query(ctx context.Context, sql string, args ...any) (TxRows, error) {
	if rows, err := tx.tx.Query(ctx, sql, args...); err != nil {
		return nil, err
	} else {
		return &PGTxRows{rows}, nil
	}
}

func (tx *PGTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

func (tx *PGTx) Rollback(ctx context.Context) error {
	return tx.tx.Rollback(ctx)
}

func (rows *PGTxRows) FieldNames() ([]string, error) {
	fields := rows.FieldDescriptions()
	fieldNames := make([]string, 0, len(fields))
	for _, field := range fields {
		fieldNames = append(fieldNames, field.Name)
	}

	return fieldNames, nil
}
