package db

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cuityhj/cement/reflector"

	"github.com/cuityhj/gorest/resource"
)

type RStore struct {
	DB
	meta *ResourceMeta
}

type RStoreTx struct {
	Tx
	meta *ResourceMeta
}

func NewRStore(driverName DriverName, connStr string, meta *ResourceMeta, dropSchemaList ...string) (ResourceStore, error) {
	db, err := NewDB(driverName, connStr)
	if err != nil {
		return nil, err
	}

	if recovery, err := db.IsRecoveryMode(); err != nil {
		db.Close()
		return nil, err
	} else if recovery == false {
		if err := db.InitSchema(dropSchemaList...); err != nil {
			db.Close()
			return nil, err
		}

		for _, descriptor := range meta.GetDescriptors() {
			if err := db.Exec(context.TODO(), createTableSql(descriptor)); err != nil {
				db.Close()
				return nil, err
			}
		}
	}

	return &RStore{db, meta}, nil
}

func (store *RStore) Close() {
	store.DB.Close()
}

func (store *RStore) Clean() {
	rs := store.meta.Resources()
	for i := len(rs); i > 0; i-- {
		tableName := resourceTableName(rs[i-1])
		store.DB.Exec(context.TODO(), "DROP TABLE IF EXISTS "+tableName+" CASCADE")
	}
}

func (store *RStore) Begin() (Transaction, error) {
	if tx, err := store.DB.Begin(); err != nil {
		return nil, err
	} else {
		return RStoreTx{tx, store.meta}, nil
	}
}

func (tx RStoreTx) Insert(r resource.Resource) (resource.Resource, error) {
	r.SetCreationTimestamp(time.Now())
	sql, args, err := insertSqlArgsAndID(tx.Tx.GetDriver(), tx.meta, r)
	if err != nil {
		return nil, err
	}

	if _, err = tx.Tx.Exec(context.TODO(), sql, args...); err != nil {
		return nil, err
	} else {
		return r, err
	}
}

func (tx RStoreTx) GetOwned(owner ResourceType, ownerID string, owned ResourceType) (interface{}, error) {
	goTyp, err := tx.meta.GetGoType(owned)
	if err != nil {
		return nil, err
	}

	sp := reflector.NewSlicePointer(reflect.PtrTo(goTyp))
	sql, args, err := joinSelectSqlAndArgs(tx.meta, owner, owned, ownerID)
	if err != nil {
		return nil, err
	}

	if err = tx.getWithSql(sql, args, sp); err != nil {
		return nil, err
	} else {
		return reflect.ValueOf(sp).Elem().Interface(), nil
	}
}

func (tx RStoreTx) FillOwned(owner ResourceType, ownerID string, out interface{}) error {
	r, err := reflector.GetStructPointerInSlice(out)
	if err != nil {
		return err
	}

	sql, args, err := joinSelectSqlAndArgs(tx.meta, owner, ResourceDBType(r.(resource.Resource)), ownerID)
	if err != nil {
		return err
	}

	return tx.getWithSql(sql, args, out)
}

func (tx RStoreTx) Get(typ ResourceType, conds map[string]interface{}) (interface{}, error) {
	goTyp, err := tx.meta.GetGoType(typ)
	if err != nil {
		return nil, err
	}

	sp := reflector.NewSlicePointer(reflect.PtrTo(goTyp))
	if err = tx.Fill(conds, sp); err != nil {
		return nil, err
	} else {
		return reflect.ValueOf(sp).Elem().Interface(), nil
	}
}

func (tx RStoreTx) Fill(conds map[string]interface{}, out interface{}) error {
	r, err := reflector.GetStructPointerInSlice(out)
	if err != nil {
		return err
	}

	sql, args, err := selectSqlAndArgs(tx.meta, ResourceDBType(r.(resource.Resource)), conds)
	if err != nil {
		return err
	}

	return tx.getWithSql(sql, args, out)
}

func (tx RStoreTx) getWithSql(sql string, args []interface{}, out interface{}) error {
	rows, err := tx.Tx.Query(context.TODO(), sql, args...)
	if err != nil {
		return err
	}

	return rowsToResources(rows, out)
}

func (tx RStoreTx) Exists(typ ResourceType, conds map[string]interface{}) (bool, error) {
	sql, params, err := existsSqlAndArgs(tx.meta, typ, conds)
	if err != nil {
		return false, err
	}

	return tx.existsWithSql(sql, params...)
}

func (tx RStoreTx) existsWithSql(sql string, params ...interface{}) (bool, error) {
	rows, err := tx.Tx.Query(context.TODO(), sql, params...)
	if err != nil {
		return false, err
	}

	var exist bool
	for rows.Next() {
		if err := rows.Scan(&exist); err != nil {
			return false, err
		}
	}

	return exist, nil
}

func (tx RStoreTx) Count(typ ResourceType, conds map[string]interface{}) (int64, error) {
	sql, params, err := countSqlAndArgs(tx.meta, typ, conds)
	if err != nil {
		return 0, err
	}

	return tx.countWithSql(sql, params...)
}

func (tx RStoreTx) CountEx(typ ResourceType, sql string, params ...interface{}) (int64, error) {
	if tx.meta.Has(typ) == false {
		return 0, fmt.Errorf("unknown resource type %v", typ)
	}

	return tx.countWithSql(sql, params...)
}

func (tx RStoreTx) countWithSql(sql string, params ...interface{}) (int64, error) {
	rows, err := tx.Tx.Query(context.TODO(), sql, params...)
	if err != nil {
		return 0, err
	}

	var count int64
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}

	return count, nil
}

func (tx RStoreTx) Update(typ ResourceType, nv map[string]interface{}, conds map[string]interface{}) (int64, error) {
	sql, args, err := updateSqlAndArgs(tx.meta, typ, nv, conds)
	if err != nil {
		return 0, err
	}

	return tx.Tx.Exec(context.TODO(), sql, args...)
}

func (tx RStoreTx) Delete(typ ResourceType, conds map[string]interface{}) (int64, error) {
	sql, args, err := deleteSqlAndArgs(tx.meta, typ, conds)
	if err != nil {
		return 0, err
	}

	return tx.Tx.Exec(context.TODO(), sql, args...)
}

func (tx RStoreTx) Exec(sql string, params ...interface{}) (int64, error) {
	return tx.Tx.Exec(context.TODO(), sql, params...)
}

func (tx RStoreTx) GetEx(typ ResourceType, sql string, params ...interface{}) (interface{}, error) {
	rt, err := tx.meta.GetGoType(typ)
	if err != nil {
		return nil, err
	}

	sp := reflector.NewSlicePointer(reflect.PtrTo(rt))
	if err = tx.FillEx(sp, sql, params...); err != nil {
		return nil, err
	} else {
		return reflect.ValueOf(sp).Elem().Interface(), nil
	}
}

func (tx RStoreTx) FillEx(out interface{}, sql string, params ...interface{}) error {
	return tx.getWithSql(sql, params, out)
}

func (tx RStoreTx) Commit() error {
	return tx.Tx.Commit(context.TODO())
}

func (tx RStoreTx) Rollback() error {
	return tx.Tx.Rollback(context.TODO())
}
