package gmlock

import (
	"context"
	"github.com/soulgm/daog"
	txrequest "github.com/soulgm/daog/tx"
	"os"
	"time"
)

type ShedLock struct {
	Name      string
	LockUntil time.Time
	LockedAt  time.Time
	LockedBy  string
}

var shedlockFields = struct {
	Name      string
	LockUntil string
	LockedAt  string
	LockedBy  string
}{
	"name",
	"lock_until",
	"locked_at",
	"locked_by",
}

type DBLocker struct {
	db  daog.Datasource
	dao daog.QuickDao[ShedLock]
}

func NewDbLocker(db daog.Datasource) *DBLocker {
	return NewDbLockerWithTableName(db, "shedlock")
}

func NewDbLockerWithTableName(db daog.Datasource, tableName string) *DBLocker {
	return &DBLocker{db: db, dao: daog.NewBaseQuickDao(buildShedlockMeta(tableName))}
}

func (l *DBLocker) DoLock(ctx context.Context, name string, lockMilli int64) bool {
	return l.insert(ctx, name, lockMilli) || l.update(ctx, name, lockMilli)
}
func (l *DBLocker) UnLock(ctx context.Context, name string) bool {
	result, _ := daog.AutoTransWithResult(func() (*daog.TransContext, error) {
		return daog.NewTransContext(l.db, txrequest.RequestWrite, ctx.Value("trace-id").(string))
	}, func(tc *daog.TransContext) (bool, error) {
		now := time.Now()
		modifier := daog.NewModifier()
		modifier.Add(shedlockFields.LockUntil, now)

		matcher := daog.NewMatcher()
		matcher.Eq(shedlockFields.Name, name)
		matcher.Gt(shedlockFields.LockUntil, now)

		tc.LogSql = false
		cnt, err := l.dao.UpdateByModifier(tc, modifier, matcher)
		if err != nil {
			return false, err
		}

		return cnt > 0, nil
	})
	return result
}

func (l *DBLocker) DeLock(ctx context.Context, name string) bool {
	result, _ := daog.AutoTransWithResult(func() (*daog.TransContext, error) {
		return daog.NewTransContext(l.db, txrequest.RequestWrite, ctx.Value("trace-id").(string))
	}, func(tc *daog.TransContext) (bool, error) {
		matcher := daog.NewMatcher()
		matcher.Eq(shedlockFields.Name, name)

		tc.LogSql = false
		cnt, err := l.dao.DeleteByMatcher(tc, matcher)
		if err != nil {
			return false, err
		}
		return cnt > 0, nil
	})
	return result
}

func (l *DBLocker) OnceLock(ctx context.Context, name string, lockMilli int64, action func()) bool {
	if !l.DoLock(ctx, name, lockMilli) {
		return false
	}

	defer func() {
		if !l.DeLock(ctx, name) {
		}
	}()
	action()
	return true
}

func (l *DBLocker) insert(ctx context.Context, name string, lockMilli int64) bool {
	result, _ := daog.AutoTransWithResult(func() (*daog.TransContext, error) {
		return daog.NewTransContext(l.db, txrequest.RequestWrite, ctx.Value("trace-id").(string))
	}, func(tc *daog.TransContext) (bool, error) {
		_, err := l.dao.Insert(tc, &ShedLock{
			Name:      name,
			LockUntil: time.Now().Add(time.Millisecond * time.Duration(lockMilli)),
			LockedAt:  time.Now(),
			LockedBy:  localHostName(),
		})
		if err != nil {
			return false, err
		}
		return true, nil
	})
	return result
}

func (l *DBLocker) update(ctx context.Context, name string, lockMilli int64) bool {
	result, _ := daog.AutoTransWithResult(func() (*daog.TransContext, error) {
		return daog.NewTransContext(l.db, txrequest.RequestWrite, ctx.Value("trace-id").(string))
	}, func(tc *daog.TransContext) (bool, error) {
		now := time.Now()
		modifier := daog.NewModifier()
		modifier.Add(shedlockFields.LockedAt, now)
		modifier.Add(shedlockFields.LockedBy, localHostName())
		modifier.Add(shedlockFields.LockUntil, now.Add(time.Duration(lockMilli)*time.Millisecond))
		matcher := daog.NewMatcher()
		matcher.Eq(shedlockFields.Name, name)
		matcher.Lte(shedlockFields.LockUntil, now)

		tc.LogSql = false
		cnt, err := l.dao.UpdateByModifier(tc, modifier, matcher)
		if err != nil {
			return false, err
		}
		return cnt > 0, nil
	})
	return result
}

func localHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return hostname
}

func buildShedlockMeta(table string) *daog.TableMeta[ShedLock] {
	if table == "" {
		table = "shedlock"
	}

	return &daog.TableMeta[ShedLock]{
		Table: table,
		Columns: []string{
			shedlockFields.Name,
			shedlockFields.LockUntil,
			shedlockFields.LockedAt,
			shedlockFields.LockedBy,
		},
		LookupFieldFunc: func(columnName string, ins *ShedLock, point bool) any {
			if shedlockFields.Name == columnName {
				if point {
					return &ins.Name
				}
				return ins.Name
			}
			if shedlockFields.LockUntil == columnName {
				if point {
					return &ins.LockUntil
				}
				return ins.LockUntil
			}
			if shedlockFields.LockedAt == columnName {
				if point {
					return &ins.LockedAt
				}
				return ins.LockedAt
			}
			if shedlockFields.LockedBy == columnName {
				if point {
					return &ins.LockedBy
				}
				return ins.LockedBy
			}

			return nil
		},
	}
}
