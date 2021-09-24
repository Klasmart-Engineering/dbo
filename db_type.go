package dbo

type DBType string

const (
	MySQL DBType = "mysql"
)

func (t DBType) String() string {
	return string(t)
}
