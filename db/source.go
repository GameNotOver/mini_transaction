package db

import (
	"context"
	"gorm.io/gorm"
)

// Source 代表数据源.
type Source interface {
	// 获取写库名.
	getWriteDBName(context.Context) string
	// 获取写库.
	getWriteDB(context.Context) *gorm.DB
	// 获取读库名.
	getReadDBName(context.Context) string
	// 获取读库.
	getReadDB(context.Context) *gorm.DB
}

// source 代表数据源.
type source struct {
	writeDBName func(context.Context) string
	writeDB     func(context.Context) *gorm.DB
	readDBName  func(context.Context) string
	readDB      func(context.Context) *gorm.DB
}

// NewSource 创建单库数据源.
func NewSource(name string, db *gorm.DB) Source {
	return NewWriteReadSource(name, db, name, db)
}

// NewWriteReadSource 创建读写分离数据源.
func NewWriteReadSource(
	writeDBName string, writeDB *gorm.DB,
	readDBName string, readDB *gorm.DB,
) Source {
	return NewWriteReadSourceWithFunc(
		func(_ context.Context) string { return writeDBName },
		func(_ context.Context) *gorm.DB { return writeDB },
		func(_ context.Context) string { return readDBName },
		func(_ context.Context) *gorm.DB { return readDB },
	)
}

// NewSourceWithFunc 通过工厂函数创建数据源.
func NewSourceWithFunc(
	name func(context.Context) string,
	db func(context.Context) *gorm.DB,
) Source {
	return &source{
		writeDBName: name,
		writeDB:     db,
		readDBName:  name,
		readDB:      db,
	}
}

// NewWriteReadSourceWithFunc 通过读写库工程函数创建数据源.
func NewWriteReadSourceWithFunc(
	writeDBName func(context.Context) string,
	writeDB func(context.Context) *gorm.DB,
	readDBName func(context.Context) string,
	readDB func(context.Context) *gorm.DB,
) Source {
	return &source{
		writeDBName: writeDBName,
		writeDB:     writeDB,
		readDBName:  readDBName,
		readDB:      readDB,
	}
}

func (s *source) getWriteDBName(ctx context.Context) string {
	return s.writeDBName(ctx)
}

func (s *source) getWriteDB(ctx context.Context) *gorm.DB {
	return s.writeDB(ctx)
}

func (s *source) getReadDBName(ctx context.Context) string {
	return s.readDBName(ctx)
}

func (s *source) getReadDB(ctx context.Context) *gorm.DB {
	return s.readDB(ctx)
}
