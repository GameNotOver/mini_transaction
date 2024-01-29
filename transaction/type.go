package transaction

import (
	"context"
	"sync"
)

// Manager 定义事务管理器.
//
// 此事务管理器具有跨层事务能力.
//
// 具体实现由资源提供方提供，如: db.Provider 的具体实现.
type Manager interface {
	// InTransaction context 是否在事务内.
	InTransaction(ctx context.Context) bool

	// Transaction 事务内执行回调.
	//
	// 事务在 context 进行标记.
	// 回调执行 panic 时，事务正确回滚.
	//
	// Transaction 可嵌套使用, Transaction 实现为 SavePoint.
	//
	// 回调 context 不要在新 goroutine 或回调范围外使用.
	//
	// 新的 goroutine 或 callback 外使用回调中的 context，使用 EscapeTransaction
	// 清除标记.
	Transaction(ctx context.Context, callback func(context.Context) error) error

	// MustTransaction 事务内执行回调.
	//
	// 事务行为同 Transaction.
	//
	// 事务异常以 panic 的形式抛出.
	MustTransaction(ctx context.Context, callback func(context.Context))

	// EscapeTransaction 使回调逃脱当前事务.
	//
	// 回调 context 事务标记已被清除.
	//
	// 逃脱当前事务后, OnCommitted 注册失效. 需要开启新事务才可注册.
	EscapeTransaction(ctx context.Context, callback func(context.Context) error) error

	// OnCommitted 事务提交成功后回调.
	//
	// 注册成功返回 true, 注册失败返回 false.
	//
	// 当前事务及其上级事务都成功时回调.
	//
	// OnCommitted 需在 Transaction callback 中使用回调的 context 进行注册.
	OnCommitted(ctx context.Context, callback func(context.Context)) bool

	// OnRollbacked 事务回滚后回调.
	//
	// 注册成功返回 true, 注册失败返回 false.
	//
	// 当前事务或上级事务回滚时回调.
	//
	// OnRollbacked 需在 Transaction callback 中使用回调的 context 进行注册.
	OnRollbacked(ctx context.Context, callback func(context.Context, error)) bool
}

// TransContext 代表事务上下文.
//
// 用于事务管理器的具体实现从上下文中获取事务 DB.
type TransContext interface {
	// GetTransDB 获取事务 DB.
	GetTransDB() interface{}
	// InTransaction 判断是否在事务内.
	InTransaction() bool
}

// transContext 实现事务上下文.
type transContext struct {
	// 根节点属性.
	mut                   sync.Mutex
	onCommittedCallbacks  []func()
	onRollbackedCallbacks []func()

	// 父节点. 父节点为 nil，则为根节点.
	parent *transContext
	// 当前事务 DB 实例.
	db interface{}

	// 标记事务已结束.
	done bool
	// 是否 panic. 事务开始前设置为 true , 事务结束时设置为 false.
	panicked bool
	// 当前事务执行结果是否异常.
	err error
}

var _ TransContext = new(transContext)

func (t *transContext) GetTransDB() interface{} {
	return t.db
}

func (t *transContext) InTransaction() bool {
	if t == nil {
		return false
	}
	if !t.isRoot() {
		return t.parent.InTransaction()
	}
	return !t.done
}

// Start 标记新事务开启.
func (t *transContext) Start(db interface{}) *transContext {
	return &transContext{parent: t, db: db, panicked: true}
}

// End 标记当前事务结束.
func (t *transContext) End(panicked bool, err error) {
	if t == nil {
		return
	}
	t.panicked = panicked
	t.err = err

	// 非根事务节点不触发.
	if !t.isRoot() {
		return
	}
	t.doOnCommittedCallbacks()
	t.doOnRollbackedCallbacks()
}

// doOnCommittedCallbacks 处理注册到根节点的回调.
func (t *transContext) doOnCommittedCallbacks() {
	var callbacks []func()

	t.mut.Lock()
	for _, callback := range t.onCommittedCallbacks {
		callbacks = append(callbacks, callback)
	}
	t.mut.Unlock()

	for _, callback := range callbacks {
		callback()
	}
}

// doOnRollbackedCallbacks 处理注册到根节点的回调.
func (t *transContext) doOnRollbackedCallbacks() {
	var callbacks []func()

	t.mut.Lock()
	for _, callback := range t.onRollbackedCallbacks {
		callbacks = append(callbacks, callback)
	}
	t.mut.Unlock()

	for _, callback := range callbacks {
		callback()
	}
}

func (t *transContext) isRoot() bool {
	return t.parent == nil
}

func (t *transContext) isCommitted() bool {
	if t == nil {
		return true
	}
	if t.panicked || t.err != nil {
		return false
	}
	return t.parent.isCommitted()
}

func (t *transContext) isRollbacked() error {
	if t == nil {
		return nil
	}
	if t.err != nil {
		return t.err
	}
	return t.parent.isRollbacked()
}

// OnCommitted 添加事务提交回调.
func (t *transContext) OnCommitted(callback func()) {
	callbackFunc := func() {
		if t.isCommitted() {
			callback()
		}
	}
	if t.parent == nil {
		t.mut.Lock()
		defer t.mut.Unlock()

		t.onCommittedCallbacks = append(t.onCommittedCallbacks, callbackFunc)
		return
	}
	t.parent.OnCommitted(callbackFunc)
}

// OnRollbacked 添加事务回滚回调.
func (t *transContext) OnRollbacked(callback func(error)) {
	callbackFunc := func() {
		if err := t.isRollbacked(); err != nil {
			callback(err)
		}
	}
	if t.parent == nil {
		t.mut.Lock()
		defer t.mut.Unlock()

		t.onRollbackedCallbacks = append(t.onRollbackedCallbacks, callbackFunc)
		return
	}
	t.parent.OnRollbacked(callback)
}
