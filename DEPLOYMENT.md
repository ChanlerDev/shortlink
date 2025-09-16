# 短链接服务部署指南

基于 GitHub Actions 自动构建的 Docker 镜像部署方案。

## 🚀 自动化构建

### GitHub Actions 配置
- **触发条件**: Push 到 `main` 分支或创建 `v*` 标签
- **镜像仓库**: `ghcr.io/merryfling/shortlink`
- **标签策略**: 
  - `main` 分支 → `latest` 标签
  - Git 标签 → 对应版本标签（如 `v1.0.0`）

### 手动触发构建
在 GitHub 仓库页面，进入 Actions → Build and Push Docker Image → Run workflow

## 📦 服务器部署

### 1. 最简部署（使用默认配置）

```bash
# 下载部署文件
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/docker-compose.yml
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/link.sql
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/application-docker.yaml
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/shardingsphere-config-docker.yaml

# 启动服务（使用默认密码）
docker-compose up -d
```

### 常见数据库连接错误排查

- 报错 `Public Key Retrieval is not allowed`
  - 原因：MySQL 8 默认 `caching_sha2_password`，JDBC 未允许公钥获取。
  - 解决（二选一）：
    - 在 JDBC URL 追加：`allowPublicKeyRetrieval=true&useSSL=false`
      - 示例（在 shardingsphere-config-docker.yaml 的 `jdbcUrl` 中）：
        `jdbc:mysql://shortlink-mysql:3306/db_shortlink?...&useSSL=false&allowPublicKeyRetrieval=true`
    - 或将用户改为 `mysql_native_password`：
      ```bash
      docker compose exec shortlink-mysql mysql -uroot -p$MYSQL_ROOT_PASSWORD -e \
        "ALTER USER 'linkapp'@'%' IDENTIFIED WITH mysql_native_password BY '$MYSQL_PASSWORD'; FLUSH PRIVILEGES;"
      ```
  - 同时确保：容器内连接地址使用 `shortlink-mysql:3306`。

### 2. 安全部署（推荐 - 自定义密码）

```bash
# 1. 下载部署文件
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/docker-compose.yml
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/link.sql
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/application-docker.yaml
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/shardingsphere-config-docker.yaml
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/.env.example

# 2. 设置自定义密码
cp .env.example .env
vi .env

# 编辑 .env 文件，修改以下密码：
# MYSQL_ROOT_PASSWORD=YourStrongPassword
# MYSQL_PASSWORD=YourStrongPassword  
# REDIS_PASSWORD=YourStrongPassword

# 3. 启动服务
docker-compose up -d
```

### 3. 完全自定义部署（高级用户）

```bash
# 1. 下载所有文件
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/docker-compose.yml
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/link.sql
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/application-docker.yaml
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/shardingsphere-config-docker.yaml
wget https://raw.githubusercontent.com/Merryfling/shortlink/main/.env.example

# 2. 设置环境变量
cp .env.example .env
vi .env

# 3. 编辑配置文件（根据需要修改）
vi application-docker.yaml
# 主要修改：
# - short-link.domain.default: 改为您的域名
# - spring.data.redis.password: 使用 .env 中设置的密码

vi shardingsphere-config-docker.yaml  
# 主要修改：
# - 数据库连接信息（如果需要）
# - 分表数量配置

# 4. 启动服务
docker-compose up -d
```

## 📁 目录结构

### 最简部署
```
deployment/
├── docker-compose.yml         # 服务编排文件
├── link.sql                  # 数据库表结构
├── application-docker.yaml          # 应用配置
└── shardingsphere-config-docker.yaml # 分库分表配置
```

### 安全部署（推荐）
```
deployment/
├── docker-compose.yml         # 服务编排文件
├── link.sql                  # 数据库表结构
├── application-docker.yaml          # 应用配置
├── shardingsphere-config-docker.yaml # 分库分表配置
└── .env                      # 环境变量配置（密码等）
```

### 完全自定义部署
```
deployment/
├── docker-compose.yml         # 服务编排文件
├── link.sql                  # 数据库表结构
├── .env                      # 环境变量配置
├── application-docker.yaml          # 自定义应用配置
└── shardingsphere-config-docker.yaml # 自定义分库分表配置
```

## 🗄️ 数据库自动初始化

### MySQL 容器启动时自动执行：
1. **创建数据库**: 自动创建 `db_shortlink` 数据库
2. **创建用户**: 自动创建应用用户 `linkapp` 并授权
3. **执行 SQL**: 自动执行 `link.sql` 创建所有表结构，包括：
   - 用户表（单表 `t_user`）
   - 短链接表（分表 `t_link_0` ~ `t_link_15`） 
   - 跳转表（分表 `t_link_goto_0` ~ `t_link_goto_15`）
   - 分组表（分表 `t_group_0` ~ `t_group_15`）
   - 统计相关表等

### 手动数据库操作（如需要）

如果需要手动操作数据库，可以使用以下命令：

```bash
# 1. 进入 MySQL 容器
docker-compose exec shortlink-mysql mysql -u root -p

# 2. 手动创建数据库（如果需要）
CREATE DATABASE IF NOT EXISTS db_shortlink CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

# 3. 创建应用用户并授权（如果需要）
CREATE USER IF NOT EXISTS 'linkapp'@'%' IDENTIFIED BY 'TheBestWorkLinkapp';
GRANT ALL PRIVILEGES ON db_shortlink.* TO 'linkapp'@'%';
FLUSH PRIVILEGES;

# 4. 切换到目标数据库
USE db_shortlink;

# 5. 手动执行建表脚本（从容器外）
# 退出 MySQL 客户端，然后执行：
exit

# 从宿主机执行 SQL 文件
docker-compose exec -T shortlink-mysql mysql -u linkapp -p db_shortlink < link.sql

# 6. 验证表是否创建成功
docker-compose exec shortlink-mysql mysql -u linkapp -p -e "USE db_shortlink; SHOW TABLES;"
```

### 重置数据库

如果需要重置数据库：

```bash
# 1. 停止服务
docker-compose down

# 2. 删除数据库数据卷（⚠️ 会丢失所有数据）
docker volume rm $(docker-compose config --volumes | grep mysql)

# 3. 重新启动服务（会自动重新初始化）
docker-compose up -d
```

## 🌐 网络隔离

### Docker 网络配置
所有服务运行在独立的 `shortlink-network` 网络中：
- **应用服务**: `shortlink-app`
- **MySQL 服务**: `shortlink-mysql`  
- **Redis 服务**: `shortlink-redis`

### 网络隔离优势
- ✅ 避免与宿主机其他 MySQL/Redis 服务冲突
- ✅ 服务间通过容器名通信，更安全
- ✅ 完全隔离的网络环境

## ⚙️ 配置说明

### 环境变量配置（.env 文件）

创建 `.env` 文件来管理敏感配置：

```bash
# MySQL 数据库配置
MYSQL_ROOT_PASSWORD=YourStrongPassword
MYSQL_PASSWORD=YourStrongPassword

# Redis 配置  
REDIS_PASSWORD=YourStrongPassword

# JVM 内存配置（根据服务器配置调整）
JAVA_OPTS=-Xmx1024m -Xms512m -XX:+UseG1GC -XX:MaxGCPauseMillis=200

# 应用域名（生产环境建议修改）
SHORTLINK_DOMAIN=yourdomain.com
```

### 配置优先级
1. **环境变量（.env 文件）** > **默认值**
2. **外部挂载配置文件** > **镜像内默认配置**
3. 如果同级目录存在 `application-docker.yaml`，将覆盖镜像内配置
4. 如果同级目录存在 `shardingsphere-config-docker.yaml`，将覆盖镜像内配置

### 主要配置项

#### application-docker.yaml 关键配置：
```yaml
server:
  port: 8068

short-link:
  domain:
    default: yourdomain.com  # ⭐ 改为您的域名
  rate-limit:
    create:
      rps: 500  # 创建短链接限流
    redirect:
      rps: 1000  # 跳转限流
  goto-domain:
    white-list:
      enable: true  # 是否启用域名白名单
      details:  # 允许跳转的域名列表
        - github.com
        - zhihu.com
        # 可添加更多域名

spring:
  data:
    redis:
      host: shortlink-redis  # Docker 网络内服务名
      password: ${REDIS_PASSWORD}  # 🔄 从 .env 文件读取
```

#### docker-compose.yml 环境变量：
```yaml
# 这些变量会自动从 .env 文件读取，无需手动修改 docker-compose.yml
environment:
  MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD:-YourStrongPassword}  # 🔄 从 .env 读取
  MYSQL_PASSWORD: ${MYSQL_PASSWORD:-YourStrongPassword}         # 🔄 从 .env 读取
  REDIS_PASSWORD: ${REDIS_PASSWORD:-YourStrongPassword}         # 🔄 从 .env 读取
  JAVA_OPTS: ${JAVA_OPTS:--Xmx1024m -Xms512m -XX:+UseG1GC}   # 🔄 从 .env 读取
```

## 🔧 服务管理

```bash
# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f shortlink-app
docker-compose logs -f shortlink-mysql
docker-compose logs -f shortlink-redis

# 重启应用
docker-compose restart shortlink-app

# 停止所有服务
docker-compose down

# 更新到最新版本
docker-compose pull
docker-compose up -d
```

## 🌐 访问服务

- **应用**: http://localhost:8068
- **MySQL**: localhost:3306 
  - Root用户: `root` / `YourStrongPassword`
  - 应用用户: `linkapp` / `YourStrongPassword`
  - 数据库: `db_shortlink`
- **Redis**: localhost:6379 (密码: `YourStrongPassword`)

## 📊 生产环境优化

### 1. 修改默认密码
```bash
# 编辑 docker-compose.yml
vi docker-compose.yml

# 修改以下环境变量：
environment:
  MYSQL_ROOT_PASSWORD: YourStrongPassword
  MYSQL_PASSWORD: YourStrongPassword

# 同步修改 application-docker.yaml（如果使用自定义配置）
spring:
  data:
    redis:
      password: YourStrongPassword
```

### 2. 域名配置
```bash
# 编辑 application-docker.yaml
vi application-docker.yaml

# 修改域名设置
short-link:
  domain:
    default: yourdomain.com  # 改为您的实际域名
```

### 3. 防火墙设置
```bash
# Ubuntu/Debian
sudo ufw allow 8068

# CentOS/RHEL
sudo firewall-cmd --permanent --add-port=8068/tcp
sudo firewall-cmd --reload
```

### 4. Nginx 反向代理（推荐）
```nginx
server {
    listen 80;
    server_name yourdomain.com;
    
    location / {
        proxy_pass http://localhost:8068;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### 5. JVM 内存调优
```bash
# 根据服务器配置调整内存设置
vi docker-compose.yml

# 不同配置的推荐设置：
# 小型服务器 (2GB RAM): -Xmx512m -Xms256m
# 中型服务器 (4GB RAM): -Xmx1024m -Xms512m （默认）
# 大型服务器 (8GB+ RAM): -Xmx2048m -Xms1024m

environment:
  - JAVA_OPTS=-Xmx1024m -Xms512m -XX:+UseG1GC -XX:MaxGCPauseMillis=200
```

## 🔍 健康检查和监控

### 检查服务状态
```bash
# 检查应用健康状态
curl http://localhost:8068/

# 检查数据库连接
docker-compose exec shortlink-mysql mysql -u linkapp -p -e "USE db_shortlink; SHOW TABLES;"

# 检查 Redis 连接
docker-compose exec shortlink-redis redis-cli -a YourStrongPassword ping
```

### 查看数据库结构
```bash
# 进入 MySQL 容器
docker-compose exec shortlink-mysql mysql -u linkapp -p db_shortlink

# 查看所有表
SHOW TABLES;

# 查看分表结构（例如用户表）
SHOW TABLES LIKE 't_user%';
```

## 🎯 优势

- ✅ **自动化构建**: GitHub Actions 自动构建镜像
- ✅ **配置灵活**: 支持外部配置文件覆盖
- ✅ **数据库自动初始化**: 一键创建数据库和表结构
- ✅ **网络隔离**: 独立网络避免端口冲突
- ✅ **部署简单**: 下载文件即可启动
- ✅ **版本管理**: 通过 Git 标签管理发布版本
- ✅ **零依赖**: 服务器无需 Java、Maven 环境
