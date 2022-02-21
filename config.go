package dbo

import (
	"time"
)

// Config dbo config
type Config struct {
	ConnectionString string
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
	// DEPRECATED: always show log, you can use LogLevel to control log output
	ShowLog bool
	// DEPRECATED: always show sql
	ShowSQL            bool
	DBType             DBType
	TransactionTimeout time.Duration
	LogLevel           LogLevel
	SlowThreshold      time.Duration
}

func getDefaultConfig() *Config {
	return &Config{
		DBType:             MySQL,
		ShowLog:            true,
		ShowSQL:            true,
		TransactionTimeout: time.Second * 3,
		// default log level, include INFO & WARN & ERROR logs
		LogLevel: Info,
	}
}

// Option dbo option
type Option func(*Config)

func WithConnectionString(connectionString string) Option {
	return func(c *Config) {
		c.ConnectionString = connectionString
	}
}

func WithDBType(dbType DBType) Option {
	return func(c *Config) {
		c.DBType = dbType
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

func WithLogLevel(logLevel LogLevel) Option {
	return func(c *Config) {
		c.LogLevel = logLevel
	}
}
