package db

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"
)

var (
	ErrWriteDBNotConfigured = errors.New("write database not configured")
)

// MultiRWOptions 定义多主从配置.
type MultiRWOptions map[string]*RWOptions

// Dialector 定义数据库配置与方言转换函数.
type Dialector func(*Options) (gorm.Dialector, error)

// RWOptions 定义主从配置.
//
// 支持一主一从模式,一主多从由基础设施支持.
type RWOptions struct {
	// 主库配置.
	Write *Options `yaml:"write" mapstructure:"write"`
	// 从库配置.
	Read *Options `yaml:"read" mapstructure:"read"`
}

// Options 定义数据库配置.
type Options struct {
	// 地址信息.
	Host string `yaml:"host" mapstructure:"host"`
	Port int    `yaml:"port" mapstructure:"port"`

	// 认证配置项.
	DBName   string `yaml:"db_name" mapstructure:"db_name"`
	UserName string `yaml:"username" mapstructure:"username"`
	Password string `yaml:"password" mapstructure:"password"`

	// 超时配置项.
	TimeoutInMills      uint `yaml:"timeout_in_mills" mapstructure:"timeout_in_mills"`
	ReadTimeoutInMills  uint `yaml:"read_timeout" mapstructure:"read_timeout"`
	WriteTimeoutInMills uint `yaml:"write_timeout" mapstructure:"write_timeout"`

	// 连接池配置项.
	MaxIdleConns uint `yaml:"max_idle_conns" mapstructure:"max_idle_conns"`
	MaxOpenConns uint `yaml:"max_open_conns" mapstructure:"max_open_conns"`
}

// OpenDBs 创建数据库连接列表.
func (o MultiRWOptions) OpenDBs(dial Dialector, config *gorm.Config) (map[string]*gorm.DB, error) {
	dbs := make(map[string]*gorm.DB)
	for key, opt := range o {
		if opt == nil {
			continue
		}
		db, err := opt.OpenDB(dial, config)
		if err != nil {
			return nil, err
		}
		dbs[key] = db
	}
	return dbs, nil
}

// ToSource 转换配置为数据源.
func (o MultiRWOptions) ToSource(dial Dialector, config *gorm.Config, router func(context.Context) string) (Source, error) {
	dbs, err := o.OpenDBs(dial, config)
	if err != nil {
		return nil, err
	}
	return NewSourceWithFunc(router, RouteWithKey(dbs, router)), nil
}

// OpenDB 创建数据库连接.
func (o *RWOptions) OpenDB(dial Dialector, config *gorm.Config) (*gorm.DB, error) {
	if o.Write == nil {
		return nil, ErrWriteDBNotConfigured
	}
	db, err := o.Write.OpenDB(dial, config)
	if err != nil {
		return nil, err
	}

	if o.Read == nil {
		return db, nil
	}
	rd, err := o.Read.openDB(dial)
	if err != nil {
		return nil, err
	}

	if err = db.Use(dbresolver.Register(dbresolver.Config{
		Replicas: []gorm.Dialector{rd},
	})); err != nil {
		return nil, err
	}
	return db, nil
}

func (o *Options) openDB(dial Dialector) (gorm.Dialector, error) {
	dl, err := dial(o)
	if err != nil {
		return nil, err
	}
	return dl, nil
}

// Open 创建数据库连接.
func (o *Options) OpenDB(dial Dialector, config *gorm.Config) (*gorm.DB, error) {
	if config == nil {
		config = &gorm.Config{}
	}
	dl, err := o.openDB(dial)
	if err != nil {
		return nil, err
	}
	return gorm.Open(dl, config)
}

func (o *Options) fullName() string {
	if o == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d/%s", o.Host, o.Port, o.DBName)
}

// ToSource 转换配置为数据源.
func (o *Options) ToSource(dial Dialector, config *gorm.Config) (Source, error) {
	db, err := o.OpenDB(dial, config)
	if err != nil {
		return nil, err
	}
	return NewSource(o.fullName(), db), nil
}

// RouteWithKey 创建按 key 路由数据库工厂函数.
//
// 用于需要按照 context 路由数据库的场景.
func RouteWithKey(
	dbs map[string]*gorm.DB,
	namef func(context.Context) string,
) func(context.Context) *gorm.DB {
	return func(ctx context.Context) *gorm.DB {
		return dbs[namef(ctx)]
	}
}
