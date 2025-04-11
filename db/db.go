package db

import (
	"context"
	"fmt"

	"github.com/cuityhj/pgx/v5/pgconn"
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
	GetDriver() Driver
}

type TxRows interface {
	DBRows
	Fields() []pgconn.FieldDescription
	GetDriver() Driver
}

type Tx interface {
	Exec(context.Context, string, ...any) (int64, error)
	Query(context.Context, string, ...any) (TxRows, error)
	Commit(context.Context) error
	Rollback(context.Context) error
	GetDriver() Driver
}

type Driver uint32

const (
	DriverPostgresql Driver = 1
	DriverOpenGauss  Driver = 2
)

type DriverName string

const (
	DriverNamePostgresql DriverName = "postgresql"
	DriverNameOpenGauss  DriverName = "opengauss"
)

func NewDB(driverName DriverName, connStr string) (DB, error) {
	switch driverName {
	case DriverNamePostgresql:
		return NewPGDB(connStr)
	case DriverNameOpenGauss:
		return NewGaussDB(connStr)
	default:
		return nil, fmt.Errorf("unsupported driver %s", driverName)
	}
}
