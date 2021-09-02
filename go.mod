module gitlab.badanamu.com.cn/calmisland/dbo

go 1.14

require (
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/go-sql-driver/mysql v1.6.0
	github.com/kr/pretty v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/testify v1.5.1 // indirect
	gitlab.badanamu.com.cn/calmisland/common-log v0.1.4
	gitlab.badanamu.com.cn/calmisland/krypton v1.2.15
	gopkg.in/yaml.v3 v3.0.0-20200605160147-a5ece683394c // indirect
	gorm.io/driver/mysql v1.1.2
	gorm.io/gorm v1.21.13
)

replace (
	github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.4
	google.golang.org/grpc => google.golang.org/grpc v1.26.0
)
