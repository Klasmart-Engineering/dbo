package dbo

import (
	// mysql driver
	"context"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gitlab.badanamu.com.cn/calmisland/krypton/krconfig"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strings"
	"sync"
	"time"
)

const (
	defaultDatabase = "kidsloop"
)

var (
	globalDBO   *DBO
	globalMutex sync.Mutex
)

// MustGetDB get db context otherwise panic
func MustGetDB(ctx context.Context) *DBContext {
	dbContext, err := GetDB(ctx)
	if err != nil {
		log.Panic(ctx, "get db context failed", log.Err(err))
	}

	return dbContext
}

// GetDB get db context
func GetDB(ctx context.Context) (*DBContext, error) {
	globalMutex.Lock()
	defer globalMutex.Unlock()

	if globalDBO == nil {
		dbo, err := New()
		if err != nil {
			return nil, err
		}

		globalDBO = dbo
	}

	return globalDBO.GetDB(ctx), nil
}

// ReplaceGlobal replace global dbo instance
func ReplaceGlobal(dbo *DBO) {
	globalMutex.Lock()
	defer globalMutex.Unlock()

	globalDBO = dbo
}

// Config dbo config
type Config struct {
	ConnectionString   string
	MaxOpenConns       int
	MaxIdleConns       int
	ShowLog            bool
	ShowSQL            bool
	TransactionTimeout time.Duration
	LogLevel           logger.LogLevel
}

func getDefaultConfig() (*Config, error) {
	err := krconfig.Init()
	if err != nil {
		return nil, err
	}

	cfg := krconfig.CommonShareConfig()
	return &Config{
		ConnectionString:   cfg.Db.Mysql.ConnStr,
		MaxOpenConns:       cfg.Db.Mysql.Params.DbMaxOpenConn,
		MaxIdleConns:       cfg.Db.Mysql.Params.DbMaxIdleConn,
		ShowLog:            cfg.Db.Mysql.Params.DbShowLog,
		ShowSQL:            cfg.Db.Mysql.Params.Debug,
		TransactionTimeout: dbTransactionTimeout,
	}, nil
}

// DBO database operator
type DBO struct {
	db     *gorm.DB
	config *Config
}

// New create new database operator
func New(options ...Option) (*DBO, error) {
	config, err := getDefaultConfig()
	if err != nil {
		return nil, err
	}

	for _, option := range options {
		option(config)
	}
	db, err := gorm.Open(mysql.Open(config.ConnectionString))
	if err != nil {
		log.Error(context.Background(), "init mysql connection failed", log.String("conn", config.ConnectionString))
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Error(context.Background(), "get mysql DB failed", log.String("conn", config.ConnectionString))
		return nil, err
	}
	err = sqlDB.Ping()
	if err != nil {
		log.Error(context.Background(), "ping mysql datebase failed", log.String("conn", config.ConnectionString))
		return nil, err
	}

	if config.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(config.MaxOpenConns)
	}

	if config.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(config.MaxIdleConns)
	}

	dbTransactionTimeout = config.TransactionTimeout

	return &DBO{db, config}, nil
}

// New create new database operator
func NewWithConfig(options ...Option) (*DBO, error) {
	config := new(Config)
	for _, option := range options {
		option(config)
	}

	db, err := gorm.Open(mysql.Open(config.ConnectionString))
	if err != nil {
		log.Error(context.Background(), "init mysql connection failed", log.String("conn", config.ConnectionString))
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Error(context.Background(), "get mysql DB failed", log.String("conn", config.ConnectionString))
		return nil, err
	}
	err = sqlDB.Ping()
	if err != nil {
		log.Error(context.Background(), "ping mysql datebase failed", log.String("conn", config.ConnectionString))
		return nil, err
	}

	if config.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(config.MaxOpenConns)
	}

	if config.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(config.MaxIdleConns)
	}
	return &DBO{db, config}, nil
}

// Option dbo option
type Option func(*Config)

func WithConnectionString(connectionString string) Option {
	return func(c *Config) {
		c.ConnectionString = connectionString
	}
}

func WithDBName(dbName string) Option {
	return func(c *Config) {
		c.ConnectionString = strings.Replace(c.ConnectionString, defaultDatabase, dbName, -1)
	}
}

func WithMaxOpenConns(maxOpenConns int) Option {
	return func(c *Config) {
		c.MaxOpenConns = maxOpenConns
	}
}

func WithMaxIdleConns(maxIdleConns int) Option {
	return func(c *Config) {
		c.MaxIdleConns = maxIdleConns
	}
}

func WithShowLog(showLog bool) Option {
	return func(c *Config) {
		c.ShowLog = showLog
	}
}

func WithShowSQL(showSQL bool) Option {
	return func(c *Config) {
		c.ShowSQL = showSQL
	}
}

func WithTransactionTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.TransactionTimeout = timeout
	}
}

func (s DBO) GetDB(ctx context.Context) *DBContext {
	ctxDB := &DBContext{DB: s.db.WithContext(ctx)}
	if s.config.ShowSQL {
		newLogger := logger.New(
			ctxDB,
			logger.Config{
				SlowThreshold:             time.Microsecond,
				LogLevel:                  s.config.LogLevel,
				IgnoreRecordNotFoundError: true,
				Colorful:                  false,
			},
		)
		ctxDB.Logger = newLogger
	}
	return ctxDB
}
