package db

import (
	"context"
)

type recovery struct {
	PgIsInRecovery bool
}

func DBIsRecoveryMode(db DB) (bool, error) {
	rows, err := db.Query(context.TODO(), "select pg_is_in_recovery()")
	if err != nil {
		return false, err
	}

	var rs []*recovery
	for rows.Next() {
		var r recovery
		if err := rows.Scan(&r.PgIsInRecovery); err != nil {
			return false, err
		} else {
			rs = append(rs, &r)
		}
	}

	return len(rs) == 1 && rs[0].PgIsInRecovery, nil
}
