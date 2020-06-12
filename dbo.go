package dbo

import (
	// mysql driver
	"context"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"gitlab.badanamu.com.cn/calmisland/common-cn/logger"
	"gitlab.badanamu.com.cn/calmisland/krypton/shareconfig"
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
		logger.WithError(err).Panic("get db context failed")
	}

	return dbContext
}

// GetDB get db context
func GetDB(ctx context.Context) (*DBContext, error) {
	if globalDBO != nil {
		return globalDBO.GetDB(ctx), nil
	}

	globalMutex.Lock()
	defer globalMutex.Unlock()

	if globalDBO != nil {
		return globalDBO.GetDB(ctx), nil
	}

	dbo, err := New()
	if err != nil {
		return nil, err
	}

	globalDBO = dbo

	return globalDBO.GetDB(ctx), nil
}

// Config dbo config
type Config struct {
	ConnectionString string
	MaxOpenConns     int
	MaxIdleConns     int
	ShowLog          bool
	ShowSQL          bool
}

func getDefaultConfig() (*Config, error) {
	shareConfig, err := shareconfig.LoadMySQLConfig(defaultDatabase)
	if err != nil {
		return nil, err
	}

	return &Config{
		ConnectionString: shareConfig.ConnectionString,
		MaxOpenConns:     shareConfig.Params.MaxOpenConns,
		MaxIdleConns:     shareConfig.Params.MaxIdleConns,
		ShowLog:          shareConfig.Params.ShowLog,
		ShowSQL:          shareConfig.Params.ShowSQL,
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

	db, err := gorm.Open("mysql", config.ConnectionString)
	if err != nil {
		logger.WithError(err).
			WithField("conn", config.ConnectionString).
			Error("init mysql connection failed")
		return nil, err
	}

	err = db.DB().Ping()
	if err != nil {
		logger.WithError(err).
			WithField("conn", config.ConnectionString).
			Error("ping mysql datebase failed")
		return nil, err
	}

	if config.MaxOpenConns > 0 {
		db.DB().SetMaxOpenConns(config.MaxOpenConns)
	}

	if config.MaxIdleConns > 0 {
		db.DB().SetMaxIdleConns(config.MaxIdleConns)
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

func (s DBO) GetDB(ctx context.Context) *DBContext {
	ctxDB := &DBContext{DB: s.db.New(), ctx: ctx}
	if s.config.ShowSQL {
		ctxDB.LogMode(true)
		ctxDB.SetLogger(ctxDB)
	} else {
		ctxDB.LogMode(false)
	}

	return ctxDB
}
