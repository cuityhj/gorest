package db

import (
	"context"
	"fmt"
)

type DBRows interface {
	Next() bool
	Scan(...any) error
}

type DB interface {
	IsRecoveryMode() (bool, error)
	InitSchema(...string) error
	Exec(context.Context, string, ...any) error
	Query(context.Context, string, ...any) (DBRows, error)
	Begin() (Tx, error)
	Close() error
}

type TxRows interface {
	DBRows
	FieldNames() ([]string, error)
}

type Tx interface {
	Exec(context.Context, string, ...any) (int64, error)
	Query(context.Context, string, ...any) (TxRows, error)
	Commit(context.Context) error
	Rollback(context.Context) error
}

type DriverName string

const (
	DriverNamePostgresql DriverName = "postgresql"
	DriverNameGaussDB    DriverName = "gaussdb"
)

func NewDB(driverName DriverName, connStr string) (DB, error) {
	switch driverName {
	case DriverNamePostgresql:
		return NewPGDB(connStr)
	case DriverNameGaussDB:
		return NewGaussDB(driverName, connStr)
	default:
		return nil, fmt.Errorf("unsupported driver %s", driverName)
	}
}
