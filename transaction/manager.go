package transaction

import (
	"context"
	"fmt"
)

type manager struct {
	// 返回在 context 中存储事务上下文的 Key.
	ctxKeyF func(context.Context) interface{}
	// 通过 context 查找 DB, 非事务上下文中 DB.
	lookupDB func(context.Context) interface{}
	// 实现事务开启并通过回调返回新 DB.
	transaction func(ctx context.Context, db interface{}, callback func(db interface{}, bind func(context.Context)) error) error
}

func NewManager(
// 事务上下文在 context 中存储的 key.
	ctxKeyF func(context.Context) interface{},
// 实现通过 context 查找 DB, 非事务上下文中 DB.
	lookupDB func(context.Context) interface{},
// 实现事务执行并通过回调返回新 DB.
	transaction func(ctx context.Context, db interface{}, callback func(db interface{}, bindCtx func(context.Context)) error) error,
) Manager {
	return &manager{
		ctxKeyF:     ctxKeyF,
		lookupDB:    lookupDB,
		transaction: transaction,
	}
}

func (m *manager) InTransaction(ctx context.Context) bool {
	transCtx := m.findTransContext(ctx)
	return transCtx.InTransaction()
}

func (m *manager) Transaction(ctx context.Context, callback func(context.Context) error) error {
	var transCtx *transContext
	defer func() {
		if transCtx == nil {
			return
		}
		transCtx.done = true

		// 没有回滚监测，不捕获 panic.
		if len(transCtx.onRollbackedCallbacks) <= 0 {
			return
		}

		// panic 时的 Rollback 回调.
		if e := recover(); e != nil {
			err, ok := e.(error)
			if !ok {
				err = fmt.Errorf("panic: %v", e)
			}
			transCtx.End(true, err)
			// 重新触发 panic.
			panic(e)
		}
	}()

	prevTransCtx, db := m.findDBAndTransContext(ctx)
	err := m.transaction(ctx, db, func(db interface{}, bindCtx func(context.Context)) error {
		transCtx = prevTransCtx.Start(db)
		ctx = m.setTransContext(ctx, transCtx)
		if bindCtx != nil {
			bindCtx(ctx)
		}
		return callback(ctx)
	})
	transCtx.End(false, err)
	return err
}

func (m *manager) MustTransaction(ctx context.Context, callback func(context.Context)) {
	if err := m.Transaction(ctx, func(ctx context.Context) error {
		callback(ctx)
		return nil
	}); err != nil {
		panic(err)
	}
}

func (m *manager) EscapeTransaction(ctx context.Context, callback func(context.Context) error) error {
	return callback(m.cleanTransContext(ctx))
}

func (m *manager) OnCommitted(ctx context.Context, callback func(context.Context)) bool {
	transCtx := m.findTransContext(ctx)
	if !transCtx.InTransaction() {
		// 未开启事务.
		return false
	}
	// 在事务外执行, 需要清理 context.
	transCtx.OnCommitted(func() { callback(m.cleanTransContext(ctx)) })
	return true
}

func (m *manager) OnRollbacked(ctx context.Context, callback func(context.Context, error)) bool {
	transCtx := m.findTransContext(ctx)
	if transCtx == nil {
		// 未开启事务.
		return false
	}
	// 在事务外执行, 需要清理 context.
	transCtx.OnRollbacked(func(err error) {
		callback(m.cleanTransContext(ctx), err)
	})
	return true
}

// findTransContext 查找事务上下文.
func (m *manager) findTransContext(ctx context.Context) *transContext {
	tc, ok := ctx.Value(m.ctxKeyF(ctx)).(*transContext)
	if !ok {
		return nil
	}
	return tc
}

// findDBAndTransContext 查找 DB 和事务上下文.
func (m *manager) findDBAndTransContext(ctx context.Context) (*transContext, interface{}) {
	tc := m.findTransContext(ctx)
	if tc.InTransaction() {
		return tc, tc.db
	}
	db := m.lookupDB(ctx)
	if db == nil {
		panic("matching database not found")
	}
	return nil, db
}

func (m *manager) setTransContext(ctx context.Context, tc *transContext) context.Context {
	return context.WithValue(ctx, m.ctxKeyF(ctx), tc)
}

func (m *manager) cleanTransContext(ctx context.Context) context.Context {
	if !m.findTransContext(ctx).InTransaction() {
		return ctx
	}
	return context.WithValue(ctx, m.ctxKeyF(ctx), nil)
}
