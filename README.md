# SQL 聊天助手消费者 (SQLChatConsumer)

## 概述

`SQLChatConsumer` 是一个基于WebSocket的SQL查询助手，结合大语言模型(LLM)实现自然语言转SQL的功能。它能够接收用户的自然语言查询，分析意图，生成对应的SQL语句，并执行查询返回结果。

## 主要功能

1. **自然语言理解**：分析用户输入，判断是否需要执行数据库操作
2. **数据库结构获取**：实时扫描数据库表结构和字段信息
3. **SQL生成**：根据用户意图和数据库结构生成SQL语句
4. **SQL执行**：执行生成的SQL语句并返回结果
5. **结果展示**：将执行结果以友好的方式返回给前端

## 技术实现

- 使用`channels`库的`AsyncWebsocketConsumer`实现WebSocket通信
- 使用`Django ORM`的数据库连接进行SQL执行
- 通过`async/await`和`sync_to_async`实现异步处理
- 支持MySQL和SQLite等多种数据库类型

## 流程说明

1. **连接建立**：用户连接WebSocket后，发送欢迎消息
2. **处理用户输入**：
   - 分析用户输入，判断操作意图(查询/新增/更改/删除)
   - 如果是数据库操作，获取并展示数据库结构
   - 基于数据库结构生成SQL语句
   - 执行SQL并返回结果
3. **结果处理**：
   - 查询结果以JSON格式返回
   - 处理日期时间等特殊类型，确保JSON序列化
   - 区分查询操作和非查询操作(如插入/更新/删除)

## 支持的操作类型

- **查询操作**：生成SELECT语句
- **新增操作**：生成INSERT语句
- **更改操作**：生成UPDATE语句
- **删除操作**：生成DELETE语句

## 示例交互

用户可以使用以下类型的自然语言进行交互：

1. **查询**：
   - "查询所有用户信息"
   - "获取工资大于5000的员工"
   - "统计各部门的员工人数"

2. **新增**：
   - "添加一个名为张三的新用户"
   - "新增一个市场部门"

3. **更改**：
   - "将用户张三的工资改为8000"
   - "更新用户状态为已离职"

4. **删除**：
   - "删除ID为10的用户记录"
   - "清空测试数据"

## 错误处理

- 处理无效的SQL语句生成
- 处理数据库执行错误
- 处理WebSocket连接问题
- 所有错误都有日志记录

## 前端集成

前端应通过WebSocket连接与此消费者交互：

```javascript
// 创建WebSocket连接
const websocket = new WebSocket(`ws://${host}/ws/`);

// 发送消息
websocket.send(JSON.stringify({
  user_input: "查询所有用户的信息"
}));

// 接收消息
websocket.onmessage = (event) => {
  const response = JSON.parse(event.data);
  // 处理响应...
};
```

## 安全考虑

- 使用AI过滤和生成SQL可以减少SQL注入风险
- 确保数据库连接使用有限权限的用户
- 生产环境应加入更多验证和权限检查逻辑

## 依赖项

- 标准库：json, logging, re, typing
- 第三方库：asgiref, channels, django
- 项目内部模块：.util.chat (AI请求发送函数) 