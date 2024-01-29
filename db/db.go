package db

import (
	"context"
	"database/sql"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"
	"math/rand"
	"mini_transaction/transaction"
	"strconv"
)

type Command interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type Provider interface {
	// UseDB 实现通过 context 选择数据库.
	//
	// 如果在事务上下文内，返回写库.
	//
	// 不在事务上下文内时, 依据执行语句动态选择读库或写库.
	//
	// 无匹配 DB 时 panic.
	UseDB(context.Context) *gorm.DB

	// UseWriteDB 实现通过 context 选择写库.
	//
	// 无匹配 DB 时 panic.
	UseWriteDB(context.Context) *gorm.DB

	// UseCommand 返回标准库兼容的执行接口.
	UseCommand(context.Context) Command
}

func NewProvider(source Source, scopes ...func(*gorm.DB) *gorm.DB) *TransProvider {
	p := &TransProvider{
		Source:   source,
		scopes:   scopes,
		txSuffix: strconv.FormatInt(rand.Int63(), 10),
	}
	lookupDB := func(ctx context.Context) interface{} {
		return p.lookupDB(ctx, true)
	}
	p.Manager = transaction.NewManager(p.getCtxKey, lookupDB, p.transaction)
	return p
}

type TransProvider struct {
	Source
	transaction.Manager

	txSuffix string
	scopes   []func(*gorm.DB) *gorm.DB
}

var _ transaction.Manager = new(TransProvider)

type transCtxKey string

// getCtxKey 返回事务上下文存储到 context 的 Key.
//
// 事务上下文实现了 transaction.TransContext
//
// 返回的 key 需要转换为私有类型, 防止内容污染.
func (p *TransProvider) getCtxKey(ctx context.Context) interface{} {
	name := p.getWriteDBName(ctx)
	return transCtxKey(name + "." + p.txSuffix)
}

// lookupDB 查找非事务上下文 DB.
func (p *TransProvider) lookupDB(ctx context.Context, write bool) *gorm.DB {
	if write {
		return p.getWriteDB(ctx).Clauses(dbresolver.Write)
	}
	return p.getReadDB(ctx)
}

// findTransDB 查找事务上下文 DB.
func (p *TransProvider) findTransDB(ctx context.Context) *gorm.DB {
	tc, ok := ctx.Value(p.getCtxKey(ctx)).(transaction.TransContext)
	if ok && tc.InTransaction() {
		return tc.GetTransDB().(*gorm.DB)
	}
	return nil
}

// isInTransaction 判断当前 context 是否在事务上下文.
func (p *TransProvider) isInTransaction(ctx context.Context) bool {
	tc, ok := ctx.Value(p.getCtxKey(ctx)).(transaction.TransContext)
	return ok && tc.InTransaction()
}

// transaction 执行数据库事务.
func (p *TransProvider) transaction(ctx context.Context, db interface{}, callback func(db interface{}, bindCtx func(context.Context)) error) error {
	if p.isInTransaction(ctx) {
		return callback(db, func(ctx context.Context) {
			db.(*gorm.DB).Statement.Context = ctx
		})
	}
	return db.(*gorm.DB).Transaction(func(db *gorm.DB) error {
		return callback(db, func(ctx context.Context) {
			db.Statement.Context = ctx
		})
	})
}
