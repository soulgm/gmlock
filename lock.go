package gmlock

import "context"

type Locker interface {
	DoLock(ctx context.Context, name string, lockMilli int64) bool
	UnLock(ctx context.Context, name string) bool
	DeLock(ctx context.Context, name string) bool
	OnceLock(ctx context.Context, name string, lockMilli int64, action func()) bool
}
