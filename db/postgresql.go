package db

import (
	"context"

	"github.com/cuityhj/pgx/v5"
	"github.com/cuityhj/pgx/v5/pgconn"
	"github.com/cuityhj/pgx/v5/pgxpool"
)

type PGDB struct {
	pool   *pgxpool.Pool
	driver Driver
}

type PGTx struct {
	tx     pgx.Tx
	driver Driver
}

type PGTxRows struct {
	rows   pgx.Rows
	driver Driver
}

func NewPGDB(connStr string) (DB, error) {
	if pool, err := pgxpool.New(context.TODO(), connStr); err != nil {
		return nil, err
	} else {
		return &PGDB{pool: pool, driver: DriverPostgresql}, nil
	}
}

func (pg *PGDB) IsRecoveryMode() (bool, error) {
	return DBIsRecoveryMode(pg)
}

func (pg *PGDB) InitSchema(dropSchemaList ...string) error {
	return pg.Exec(context.TODO(), createSchemaIfNotExistsSql)
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
		return &PGTx{tx: tx, driver: pg.driver}, nil
	}
}

func (pg *PGDB) Close() error {
	pg.pool.Close()
	return nil
}

func (pg *PGDB) GetDriver() Driver {
	return pg.driver
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
		return &PGTxRows{rows: rows, driver: tx.driver}, nil
	}
}

func (tx *PGTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

func (tx *PGTx) Rollback(ctx context.Context) error {
	return tx.tx.Rollback(ctx)
}

func (tx *PGTx) GetDriver() Driver {
	return tx.driver
}

func (rows *PGTxRows) Next() bool {
	return rows.rows.Next()
}

func (rows *PGTxRows) Scan(fields ...any) error {
	return rows.rows.Scan(fields...)
}

func (rows *PGTxRows) Fields() []pgconn.FieldDescription {
	return rows.rows.FieldDescriptions()
}

func (rows *PGTxRows) GetDriver() Driver {
	return rows.driver
}
