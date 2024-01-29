package main

import (
	"context"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"mini_transaction/db"
	"mini_transaction/transaction"
	"time"
)

const (
	DriverName = "drive_name"
	Charset    = "utf8mb4"
)

var (
	DefaultTimeout      = 100 * time.Millisecond
	DefaultReadTimeout  = 2 * time.Second
	DefaultWriteTimeout = 5 * time.Second
)

type MysqlProviderParams struct {
	Conf Configs
}

type Configs struct {
	// Mysql配置
	Mysql map[string]db.MultiRWOptions `yaml:"mysql" mapstructure:"mysql"`
}

type TransProvider struct {
	*db.TransProvider
}

func NewTransProvider(params MysqlProviderParams) *TransProvider {
	var ret *TransProvider
	ret = newTransProvider(params)
	return ret
}

func newTransProvider(params MysqlProviderParams) *TransProvider {
	p := &TransProvider{}
	// 将两层的 mysql option 打平为一层构建provider
	mysqlOpts := db.MultiRWOptions{}
	getDBKey := func(techID, bussID string) string {
		return fmt.Sprintf("%s.%s", techID, bussID)
	}
	for techID, opts := range params.Conf.Mysql {
		for bussID, opt := range opts {
			mysqlOpts[getDBKey(techID, bussID)] = opt
		}
	}
	MyDialector := func(opts *db.Options) (gorm.Dialector, error) {
		dsn := toTcpDSN(opts)
		dl := mysql.New(mysql.Config{DriverName: DriverName, DSN: dsn})
		return dl, nil
	}
	source, err := mysqlOpts.ToSource(MyDialector, nil, func(ctx context.Context) string {
		var (
			techID string = "main"
			bussID string = "default"
		)
		return getDBKey(techID, bussID)
	})
	if err != nil {
		panic(err)
	}
	dbProvider := db.NewProvider(source, func(g *gorm.DB) *gorm.DB {
		g.PrepareStmt = true
		return g
	})
	p.TransProvider = dbProvider
	return p
}

func toTcpDSN(opts *db.Options) string {
	f := "%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=true&loc=Local&timeout=%s&readTimeout=%s&writeTimeout=%s"
	return fmt.Sprintf(f, opts.UserName, opts.Password, opts.Host,
		opts.Port, opts.DBName, Charset,
		getTimeout(opts), getReadTimeout(opts), getWriteTimeout(opts))
}

func getTimeout(opts *db.Options) time.Duration {
	if opts.TimeoutInMills > 0 {
		return time.Duration(opts.TimeoutInMills) * time.Millisecond
	}
	return DefaultTimeout
}

func getReadTimeout(opts *db.Options) time.Duration {
	if opts.ReadTimeoutInMills > 0 {
		return time.Duration(opts.ReadTimeoutInMills) * time.Millisecond
	}
	return DefaultReadTimeout
}

func getWriteTimeout(opts *db.Options) time.Duration {
	if opts.WriteTimeoutInMills > 0 {
		return time.Duration(opts.WriteTimeoutInMills) * time.Millisecond
	}
	return DefaultWriteTimeout
}

func ToTransactionManager(tp *TransProvider) transaction.Manager {
	return tp
}

func main() {
	params := MysqlProviderParams{
		Conf: Configs{},
	}
	provider := NewTransProvider(params)
	tm := ToTransactionManager(provider)
	ctx := context.Background()
	tm.MustTransaction(ctx, func(ctx context.Context) {
		// some db io
	})
}
