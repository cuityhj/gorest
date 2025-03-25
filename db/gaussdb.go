package db

import (
	"context"
	"database/sql"

	_ "gitee.com/opengauss/openGauss-connector-go-pq"
)

type GaussDB struct {
	db *sql.DB
}

type GaussTx struct {
	tx *sql.Tx
}

type GaussTxRows struct {
	*sql.Rows
}

func NewGaussDB(driverName DriverName, connStr string) (DB, error) {
	if db, err := sql.Open(string(driverName), connStr); err != nil {
		return nil, err
	} else {
		return &GaussDB{db}, nil
	}
}

func (gs *GaussDB) IsRecoveryMode() (bool, error) {
	return DBIsRecoveryMode(gs)
}

func (gs *GaussDB) InitSchema(dropSchemaList ...string) error {
	return InitDBSchema(gs, dropSchemaList...)
}

func (gs *GaussDB) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := gs.db.ExecContext(ctx, sql, args...)
	return err
}

func (gs *GaussDB) Query(ctx context.Context, sql string, args ...any) (DBRows, error) {
	return gs.db.QueryContext(ctx, sql, args...)
}

func (gs *GaussDB) Begin() (Tx, error) {
	if tx, err := gs.db.Begin(); err != nil {
		return nil, err
	} else {
		return &GaussTx{tx}, nil
	}
}

func (gs *GaussDB) Close() error {
	return gs.db.Close()
}

func (gs *GaussDB) GetDriver() Driver {
	return DriverOpenGauss
}

func (tx *GaussTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	if result, err := tx.tx.ExecContext(ctx, sql, args...); err != nil {
		return 0, err
	} else {
		return result.RowsAffected()
	}
}

func (tx *GaussTx) Query(ctx context.Context, sql string, args ...any) (TxRows, error) {
	if rows, err := tx.tx.QueryContext(ctx, sql, args...); err != nil {
		return nil, err
	} else {
		return &GaussTxRows{rows}, nil
	}
}

func (tx *GaussTx) Commit(ctx context.Context) error {
	return tx.tx.Commit()
}

func (tx *GaussTx) Rollback(ctx context.Context) error {
	return tx.tx.Rollback()
}

func (tx *GaussTx) GetDriver() Driver {
	return DriverOpenGauss
}

func (rows *GaussTxRows) FieldNames() ([]string, error) {
	return rows.Columns()
}

func (rows *GaussTxRows) GetDriver() Driver {
	return DriverOpenGauss
}
