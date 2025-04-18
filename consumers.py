# 标准库导入
import datetime
import json
import logging
import re
from typing import Dict, Any, Optional, Tuple, List, Union

# 第三方库导入
from asgiref.sync import sync_to_async, async_to_sync
from channels.generic.websocket import AsyncWebsocketConsumer
from django.db import connection

# 项目内部模块导入
from .util.chat import chat # AI请求发送函数

logger = logging.getLogger(__name__)

class SQLChatConsumer(AsyncWebsocketConsumer):
    """
    SQL聊天WebSocket消费者类
    
    处理WebSocket连接，分析用户请求，执行SQL查询，返回结果
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_retries = 4  # 生成SQL最多重试次数
    
    async def connect(self) -> None:
        """
        建立WebSocket连接
        """
        logger.info("WebSocket连接尝试")
        await self.accept()
        logger.info("WebSocket连接已接受")
        # 发送连接成功消息
        await self.send(text_data=json.dumps({
            'content': '连接成功！欢迎使用SQL AI聊天助手。',
            'is_last_message': True
        }))

    async def disconnect(self, close_code: int) -> None:
        """
        关闭WebSocket连接
        
        Args:
            close_code: WebSocket关闭代码
        """
        logger.info(f"WebSocket连接已关闭，代码: {close_code}")

    async def execute_sql(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL语句并返回结果
        
        Args:
            sql: 要执行的SQL语句
            
        Returns:
            Dict[str, Any]: 包含执行结果的字典
        """
        return await sync_to_async(self._execute_sql_sync)(sql)
        
    def _execute_sql_sync(self, sql: str) -> Dict[str, Any]:
        """
        同步执行SQL语句的辅助方法，使用字段备注替换字段名
        
        Args:
            sql: 要执行的SQL语句
            
        Returns:
            Dict[str, Any]: 包含执行结果或错误信息的字典
        """
        try:
            # 使用Django的数据库连接执行SQL
            with connection.cursor() as cursor:
                # 发送进度消息：正在执行SQL
                self.send_progress_message("⏳ 正在执行SQL查询...")
                cursor.execute(sql)
                
                try:
                    # 尝试获取结果
                    rows = cursor.fetchall()
                    # 获取列名
                    original_columns = [col[0] for col in cursor.description]
                    
                    # 发送进度消息：正在获取字段备注
                    self.send_progress_message("⏳ 正在获取字段备注信息...")
                    # 获取字段备注信息
                    column_comments = self._get_column_comments(sql)
                    
                    # 发送进度消息：正在处理查询结果
                    self.send_progress_message("⏳ 正在处理查询结果...")
                    # 创建一个新的列名列表，根据字段备注替换原始字段名
                    display_columns = []
                    for col in original_columns:
                        # 如果有备注，使用备注作为显示名，否则使用原始字段名
                        display_columns.append(column_comments.get(col, col))
                    
                    # 构建结果
                    results = []
                    total_rows = len(rows)
                    
                    # 每N行数据更新一次进度，避免发送太多消息
                    progress_step = max(1, min(100, total_rows // 10))  # 最少1行，最多每100行更新一次
                    
                    for row_index, row in enumerate(rows):
                        result_dict = {}
                        for i, value in enumerate(row):
                            # 使用备注或原始名作为键
                            key = display_columns[i]
                            
                            # 处理datetime和date类型，转换为字符串
                            if hasattr(value, 'isoformat'):  # 检查是否为datetime或date类型
                                result_dict[key] = value.isoformat()
                            # 处理其他不可JSON序列化的类型
                            elif isinstance(value, (set, frozenset)):
                                result_dict[key] = list(value)
                            elif value is None or isinstance(value, (str, int, float, bool, list, dict)):
                                result_dict[key] = value
                            else:
                                # 其他类型转换为字符串
                                result_dict[key] = str(value)
                        results.append(result_dict)
                        
                        # 按进度步长发送进度消息
                        if (row_index + 1) % progress_step == 0 or row_index + 1 == total_rows:
                            progress_percentage = ((row_index + 1) / total_rows) * 100
                            self.send_progress_message(
                                f"⏳ 正在处理数据... ({row_index + 1}/{total_rows} 行, {progress_percentage:.1f}%)"
                            )
                    
                    # 发送进度消息：查询完成
                    self.send_progress_message("⏳ 查询完成，正在返回结果...")
                    
                    return {
                        'status': 'success',
                        'results': results,
                        'type': 'query',
                        'rowCount': len(results)
                    }
                except:
                    # 如果不是SELECT语句，返回影响的行数
                    return {
                        'status': 'success',
                        'affected_rows': cursor.rowcount,
                        'type': 'non-query'
                    }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
            
    def _get_column_comments(self, sql: str) -> Dict[str, str]:
        """
        获取SQL查询涉及的字段备注
        
        Args:
            sql: SQL查询语句
            
        Returns:
            Dict[str, str]: 字段名到字段备注的映射
        """
        column_comments = {}
        
        try:
            # 尝试识别查询涉及的表名
            table_pattern = r'\bFROM\s+`?(\w+)`?'
            table_match = re.search(table_pattern, sql, re.IGNORECASE)
            
            # 查找JOIN语句中的表
            join_pattern = r'\bJOIN\s+`?(\w+)`?'
            join_matches = re.findall(join_pattern, sql, re.IGNORECASE)
            
            # 收集所有涉及的表
            tables = []
            if table_match:
                tables.append(table_match.group(1))
            tables.extend(join_matches)
            
            # 获取所有相关表的字段注释
            if connection.vendor == 'mysql' and tables:
                with connection.cursor() as comment_cursor:
                    for table_name in tables:
                        comment_cursor.execute(f"""
                            SELECT COLUMN_NAME, COLUMN_COMMENT
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = DATABASE() 
                            AND TABLE_NAME = '{table_name}'
                            AND COLUMN_COMMENT != ''
                        """)
                        for col_name, comment in comment_cursor.fetchall():
                            if comment.strip():  # 只保存有实际内容的注释
                                # 对于多表查询中的同名字段，使用"表名.字段名"作为键
                                if col_name in column_comments:
                                    # 处理字段名冲突的情况
                                    full_col_name = f"{table_name}.{col_name}"
                                    column_comments[full_col_name] = comment
                                else:
                                    column_comments[col_name] = comment
            
            # 处理列别名
            alias_pattern = r'(\w+)\.(\w+)(?:\s+AS\s+|\s+)(\w+)'
            alias_matches = re.findall(alias_pattern, sql, re.IGNORECASE)
            
            for table, col, alias in alias_matches:
                # 检查原始列是否有注释
                orig_col = f"{table}.{col}"
                if orig_col in column_comments:
                    column_comments[alias] = column_comments[orig_col]
                elif col in column_comments:
                    column_comments[alias] = column_comments[col]
                    
            # 处理简单列别名
            simple_alias_pattern = r'`?(\w+)`?\s+(?:AS\s+)?`?(\w+)`?'
            simple_alias_matches = re.findall(simple_alias_pattern, sql, re.IGNORECASE)
            
            for col, alias in simple_alias_matches:
                if col != alias and col in column_comments:
                    column_comments[alias] = column_comments[col]
                        
        except Exception as e:
            logger.warning(f"获取字段注释时出错: {str(e)}")
            
        return column_comments

    def _get_database_structure_sync(self) -> Dict[str, Dict[str, Any]]:
        """
        实时获取数据库表结构和字段注释，包括字段类型和描述
        
        Returns:
            Dict[str, Dict[str, Any]]: 数据库结构信息，格式为 {表名: {表信息}}
        """
        try:
            db_structure = {}
            
            with connection.cursor() as cursor:
                # 获取所有表
                if connection.vendor == 'mysql':
                    # MySQL
                    cursor.execute("""
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = DATABASE()
                    """)
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    # 对每个表获取列信息和注释
                    for table in tables:
                        cursor.execute(f"""
                            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}'
                        """)
                        columns = []
                        for col_name, data_type, comment in cursor.fetchall():
                            columns.append({
                                'name': col_name,
                                'type': data_type,
                                'comment': comment or f"{col_name}字段"
                            })
                        
                        # 获取表注释
                        cursor.execute(f"""
                            SELECT TABLE_COMMENT
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}'
                        """)
                        table_comment = cursor.fetchone()[0] or f"{table}表"
                        
                        db_structure[table] = {
                            'comment': table_comment,
                            'columns': columns
                        }
                else:
                    # SQLite和其他数据库
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    for table in tables:
                        cursor.execute(f"PRAGMA table_info('{table}')")
                        columns = []
                        for col in cursor.fetchall():
                            cid, name, type_name, not_null, default_value, pk = col
                            columns.append({
                                'name': name,
                                'type': type_name,
                                'comment': f"{name}字段"  # SQLite没有字段注释
                            })
                        
                        db_structure[table] = {
                            'comment': f"{table}表",
                            'columns': columns
                        }
            
            return db_structure
        except Exception as e:
            logger.error(f"获取数据库结构时出错: {str(e)}")
            return {}
    
    async def get_database_structure(self) -> Dict[str, Dict[str, Any]]:
        """
        异步获取数据库表结构
        
        Returns:
            Dict[str, Dict[str, Any]]: 数据库结构信息
        """
        return await sync_to_async(self._get_database_structure_sync)()

    async def format_db_structure_for_ai(self, db_structure: Dict[str, Dict[str, Any]]) -> str:
        """
        将数据库结构格式化为AI可读的文本
        
        Args:
            db_structure: 数据库结构信息
            
        Returns:
            str: 格式化后的Markdown文本
        """
        result = "数据库表结构信息：\n\n"
        
        for i, (table_name, table_info) in enumerate(db_structure.items(), 1):
            result += f"{i}. **{table_name}** - {table_info['comment']}\n"
            
            # 添加表格式的列信息
            result += "   | 字段名 | 类型 | 描述 |\n"
            result += "   | ------ | ---- | ---- |\n"
            
            for col in table_info['columns']:
                comment = col['comment'] if col['comment'] else f"{col['name']}字段"
                result += f"   | {col['name']} | {col['type']} | {comment} |\n"
            
            result += "\n"
        
        return result

    async def extract_sql_commands(self, text: str) -> Optional[str]:
        """
        从AI响应中提取SQL命令
        
        Args:
            text: AI生成的响应文本
            
        Returns:
            Optional[str]: 提取的SQL命令，如果未找到则返回None
        """
        # 寻找 ```sql 和 ``` 之间的内容
        sql_pattern = r'```sql\s*(.*?)\s*```'
        matches = re.findall(sql_pattern, text, re.DOTALL)
        if matches:
            return matches[0].strip()
        return None

    async def detect_operation_type(self, user_input: str) -> Dict[str, Any]:
        """
        使用AI判断用户输入的操作类型
        
        Args:
            user_input: 用户的输入文本
            
        Returns:
            Dict[str, Any]: 包含操作类型和AI响应的字典
        """
        # 获取数据库结构信息用于提示词
        db_structure = await self.get_database_structure()
        table_info = ""
        
        # 构建表信息提示
        if db_structure:
            table_info = "<数据库表>\n"
            for table_name, table_data in db_structure.items():
                table_info += f"- {table_name}：{table_data['comment']}\n"
            table_info += "</数据库表>\n\n"
        
        # 构建提示词，判断用户是否想执行数据操作
        system_prompt = f"""
        你是一个专业的数据库助手，负责理解用户意图并协助数据库操作。
        
        <任务>
        分析用户输入，判断他们是否想要执行数据库操作，并准确识别操作类型。
        </任务>
        
        {table_info}
        
        <能力>
        1. 理解用户自然语言请求并映射到数据库操作
        2. 识别四种主要操作类型：查询、新增、更改、删除
        3. 将复杂的用户表述转化为精确的操作描述
        </能力>
        
        <输出规范>
        当识别到数据库操作意图时，请使用以下格式输出：
        - 对于查询操作: ```查询\n准确描述用户想查询的内容\n```
        - 对于新增操作: ```新增\n准确描述用户想新增的内容\n```
        - 对于更改操作: ```更改\n准确描述用户想更改的内容\n```
        - 对于删除操作: ```删除\n准确描述用户想删除的内容\n```
        
        <重要说明>
        1. 对于查询操作，请使用准确的语言描述用户查询意图，不要生成具体SQL语句
        2. 确保描述清晰、具体，包含查询的主体、条件和目标
        3. 如果用户意图不清晰或不涉及数据库操作，请正常回复，不使用上述特殊格式
        4. 你可以使用markdown格式美化输出
        5. 保持描述简洁、专业，避免冗余词语
        6. 尽可能使用上述数据库表中存在的表名进行操作
        7. 今天的日期是{datetime.datetime.now().strftime('%Y-%m-%d')}
        </重要说明>
        
        <示例>
        用户: "查询一下所有工资超过5000元的员工"
        回复: "我可以帮您查询工资超过5000元的员工信息。"
        
        ```查询
        查询工资超过5000元的所有员工信息
        ```
        
        用户: "帮我添加一个新员工张三，他是市场部的，工资8000元"
        回复: "好的，我将为您添加新员工信息。"
        
        ```新增
        添加员工张三到市场部，工资8000元
        ```
        </示例>
        """
        
        reasoning, response = await chat(user_input, system_prompt)
        
        # 检查是否包含操作类型标记
        operation_patterns = {
            "新增": r'```新增\s*(.*?)\s*```',
            "查询": r'```查询\s*(.*?)\s*```',
            "更改": r'```更改\s*(.*?)\s*```',
            "删除": r'```删除\s*(.*?)\s*```'
        }
        
        for op_type, pattern in operation_patterns.items():
            match = re.search(pattern, response, re.DOTALL)
            if match:
                return {
                    "operation_type": op_type,
                    "description": match.group(1).strip(),
                    "ai_response": response
                }
        
        # 如果没有找到操作类型，返回普通回复
        return {
            "operation_type": None,
            "ai_response": response
        }

    async def generate_sql(self, operation_info: Dict[str, Any]) -> Tuple[Optional[str], str]:
        """
        使用AI生成SQL语句
        
        Args:
            operation_info: 包含操作类型和描述的字典
            
        Returns:
            Tuple[Optional[str], str]: (SQL语句, AI完整响应)
        """
        operation_type = operation_info["operation_type"]
        description = operation_info["description"]
        
        # 实时获取数据库结构
        db_structure = await self.get_database_structure()
        formatted_db_structure = await self.format_db_structure_for_ai(db_structure)
        
        # 构建提示词
        system_prompt = f"""
        你是一个精确的SQL生成专家，能够根据用户需求和数据库结构生成高质量SQL语句。

        <数据库结构>
        {formatted_db_structure}
        </数据库结构>
        
        <相关表推荐>
        基于操作描述，以下表可能与此次操作相关：
        {self._get_relevant_tables(operation_type, description, db_structure)}
        </相关表推荐>
        
        <操作信息>
        类型: {operation_type}
        描述: {description}
        </操作信息>

        <任务>
        根据操作描述和数据库结构生成正确、高效、符合标准的SQL语句。
        </任务>

        <SQL生成规则>
        1. 仅输出SQL语句，不要有任何解释或注释
        2. 使用markdown的SQL代码块格式输出，即 ```sql 开始，``` 结束
        3. 生成的SQL必须是完整、有效、可直接执行的
        4. 不要使用不存在的表或字段
        5. 确保SQL语法正确无误
        6. 针对不同操作类型使用对应的SQL语句：
           - 查询操作：使用SELECT语句
           - 新增操作：使用INSERT INTO语句
           - 更改操作：使用UPDATE语句
           - 删除操作：使用DELETE语句
        7. 按需使用WHERE子句确保操作安全
        8. 对于查询操作，选择合适的字段而非全部使用*
        9. 尽可能利用"相关表推荐"中提到的表完成操作
        10. 今天的日期是{datetime.datetime.now().strftime('%Y-%m-%d')}
        </SQL生成规则>

        <输出格式>
        ```sql
        你生成的SQL语句
        ```
        </输出格式>
        """
        
        user_prompt = f"为以下操作生成SQL语句：{operation_type} - {description}"
        reasoning, response = await chat(user_prompt, system_prompt)
        
        # 从响应中提取SQL语句
        sql = await self.extract_sql_commands(response)
        return sql, response

    async def audit_sql(self, user_input: str, sql: str, operation_info: Dict[str, Any], db_structure: Dict[str, Dict[str, Any]]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        审计生成的SQL是否符合用户期望与数据库结构
        
        Args:
            user_input: 用户原始输入
            sql: 生成的SQL语句
            operation_info: 操作信息
            db_structure: 数据库结构
            
        Returns:
            Tuple[bool, Optional[Dict[str, Any]]]: (SQL是否有效, 修改后的操作信息)
        """
        # 发送进度消息
        await self.send(text_data=json.dumps({
            'content': '',
            'loading_message': "⏳ 正在审计生成的SQL..."
        }))
        
        logger.info(f"审计SQL: {sql[:50]}...")
        
        # 将数据库结构格式化为文本
        formatted_db_structure = await self.format_db_structure_for_ai(db_structure)
        
        # 构建审计提示词
        system_prompt = f"""
        你是一位数据库SQL审计专家，负责评估生成的SQL是否符合用户需求和数据库结构。

        <数据库结构>
        {formatted_db_structure}
        </数据库结构>
        
        <用户输入>
        {user_input}
        </用户输入>
        
        <操作信息>
        类型: {operation_info.get('operation_type', 'unknown')}
        描述: {operation_info.get('description', '')}
        </操作信息>
        
        <生成的SQL>
        {sql}
        </生成的SQL>
        
        <审计任务>
        评估上述SQL是否正确实现了用户需求，并检查以下可能的问题：
        1. SQL语法错误
        2. 表名或字段名与数据库结构不匹配
        3. SQL类型(SELECT/INSERT/UPDATE/DELETE)与用户意图不符
        4. SQL操作的表与用户需求不匹配
        5. WHERE条件不合理或缺失必要条件
        6. 查询结果字段不符合用户需求
        7. 安全风险，如缺少WHERE条件的UPDATE或DELETE
        8. 今天的日期是{datetime.datetime.now().strftime('%Y-%m-%d')}
        </审计任务>
        
        <输出规范>
        请以JSON格式输出审计结果，必须遵循以下JSON Schema:
        ```json
        {{
          "$schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "required": ["is_valid", "issues", "suggested_operation_type", "suggested_description"],
          "properties": {{
            "is_valid": {{
              "type": "boolean",
              "description": "SQL是否有效，true表示有效，false表示无效"
            }},
            "issues": {{
              "type": "array",
              "items": {{
                "type": "string"
              }},
              "description": "发现的问题列表，每个问题一条，如果没有问题则为空数组"
            }},
            "suggested_operation_type": {{
              "type": "string",
              "description": "建议的操作类型，如'查询'、'新增'、'更改'、'删除'"
            }},
            "suggested_description": {{
              "type": "string",
              "description": "修正后的操作描述，如果没有问题则与原始描述相同"
            }}
          }}
        }}
        ```

        <输出示例>
        当SQL正确时：
        ```json
        {{
          "is_valid": true,
          "issues": [],
          "suggested_operation_type": "查询",
          "suggested_description": "查询所有用户信息"
        }}
        ```

        当SQL有问题时：
        ```json
        {{
          "is_valid": false,
          "issues": ["WHERE条件错误: 使用了不存在的字段'user_status'", "表名错误: 应使用'user'表而非'users'表"],
          "suggested_operation_type": "查询",
          "suggested_description": "查询状态为活跃的用户信息"
        }}
        ```
        </输出规范>
        """
        
        # 调用AI审计SQL
        reasoning, response = await chat(f"审计SQL: {sql}", system_prompt)
        
        try:
            # 尝试解析AI返回的JSON
            json_pattern = r'```json\s*(.*?)\s*```'
            json_match = re.search(json_pattern, response, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(1)
            else:
                # 没有找到JSON代码块，尝试直接解析整个响应
                json_str = response
            
            # 去除任何多余的内容，只保留JSON
            json_str = re.sub(r'^[^{]*({.*})[^}]*$', r'\1', json_str.strip(), flags=re.DOTALL)
            
            # 解析JSON
            audit_result = json.loads(json_str)
            
            # 提取审计结果
            is_valid = audit_result.get('is_valid', False)
            issues = audit_result.get('issues', [])
            
            # 记录审计结果
            if is_valid:
                logger.info("SQL审计通过")
                return True, None
            else:
                logger.warning(f"SQL审计未通过: {issues}")
                
                # 创建修改后的操作信息
                modified_operation_info = operation_info.copy()
                modified_operation_info['operation_type'] = audit_result.get('suggested_operation_type', operation_info['operation_type'])
                modified_operation_info['description'] = audit_result.get('suggested_description', operation_info['description'])
                
                # 发送审计结果通知
                issue_text = "\n- ".join([""] + issues)
                await self.send(text_data=json.dumps({
                    'content': f"⚠️ SQL审计发现以下问题:{issue_text}\n\n新的查询描述为:\n```查询\n{modified_operation_info['description']}\n```\n\n正在据此重新生成SQL...\n",
                    'is_last_message': False
                }))
                
                return False, modified_operation_info
                
        except Exception as e:
            logger.error(f"解析审计结果时出错: {str(e)}")
            # 审计失败但不阻止执行
            return True, None

    async def receive(self, text_data: str) -> None:
        """
        接收WebSocket消息主函数
        对接前端AiChat.vue
        
        Args:
            text_data: 收到的WebSocket消息
        """
        try:
            logger.info(f"收到WebSocket消息: {text_data[:100]}...")
            data = json.loads(text_data)
            user_input = data.get('user_input', '')
            
            if not user_input:
                # 尝试从其他可能的字段获取用户输入
                user_input = data.get('message', '')
            
            if not user_input:
                logger.warning("接收到空消息")
                await self.send(text_data=json.dumps({
                    'error': '未提供用户输入',
                    'is_last_message': True
                }))
                return
            
            logger.info(f"处理用户输入: {user_input[:50]}...")
            
            # 1. 响应用户输入
            await self.send(text_data=json.dumps({
                'content': '',
                'loading_message': '🔍 正在分析您的需求'
            }))
            
            # 2. 使用AI分析用户输入
            operation_info = await self.detect_operation_type(user_input)
            
            # 3. 发送AI的初步响应
            await self.send(text_data=json.dumps({
                'content': operation_info["ai_response"],
                'is_last_message': operation_info["operation_type"] is None
            }))
            
            # 如果检测到SQL操作意图
            if operation_info["operation_type"]:
                logger.info(f"检测到操作类型: {operation_info['operation_type']}")
                
                # 获取并发送数据库结构概览
                db_structure = await self.get_database_structure()
                
                await self.send(text_data=json.dumps({
                    'content': '',
                    'loading_message': f"⏳ 正在生成{operation_info['operation_type']}操作的SQL语句..."
                }))
                
                # 生成SQL
                sql, sql_response = await self.generate_sql(operation_info)
                
                # 执行SQL审计
                if sql:
                    # 将用户原始输入传递给审计函数
                    is_valid, modified_operation_info = await self.audit_sql(
                        user_input=user_input,  # 用户的原始输入
                        sql=sql,                # 生成的SQL语句
                        operation_info=operation_info,  # 操作信息
                        db_structure=db_structure       # 数据库结构
                    )
                    
                    # 如果SQL无效且有修改后的操作信息，重新生成SQL
                    retry_count = 0
                    
                    while not is_valid and modified_operation_info and retry_count < self.max_retries:
                        retry_count += 1
                        logger.info(f"根据审计结果重新生成SQL (尝试 {retry_count}/{self.max_retries})")
                        
                        # 使用修改后的操作信息重新生成SQL
                        await self.send(text_data=json.dumps({
                            'content': '',
                            'loading_message': f"⏳ 正在重新生成SQL (尝试 {retry_count}/{self.max_retries})..."
                        }))
                        
                        # 使用修改后的操作信息重新生成SQL
                        sql, sql_response = await self.generate_sql(modified_operation_info)
                        
                        # 再次审计
                        if sql:
                            is_valid, modified_operation_info = await self.audit_sql(
                                user_input=user_input, 
                                sql=sql, 
                                operation_info=modified_operation_info,
                                db_structure=db_structure
                            )
                        else:
                            # 无法生成SQL，退出循环
                            break
                
                if sql:
                    logger.info(f"生成SQL: {sql[:50]}...")
                    # 发送生成的SQL
                    await self.send(text_data=json.dumps({
                        'content': sql_response,
                        'is_last_message': False
                    }))
                    
                    # 告知正在执行SQL
                    await self.send(text_data=json.dumps({
                        'content': '',
                        'loading_message': "⚙️ 正在执行SQL...\n"
                    }))
                    
                    # 执行SQL并获取结果
                    result = await self.execute_sql(sql)
                    
                    # 针对查询类型结果进行AI优化展示
                    if result['status'] == 'success' and result['type'] == 'query' and result['rowCount'] > 0:
                        await self.send(text_data=json.dumps({
                            'content': '',
                            'loading_message': "⏳ 正在优化查询结果展示✨...\n"
                        }))
                        
                        # 使用AI优化结果展示
                        formatted_result = await self.optimize_query_result(
                            user_input=user_input,
                            sql=sql,
                            result=result,
                            operation_info=operation_info
                        )
                        
                        # 将优化后的展示添加到结果中
                        result['formatted_view'] = formatted_result
                    
                    # 将SQL执行结果添加到数据部分
                    sql_result_message = f"\n\n执行结果：\n"
                    if result['status'] == 'success':
                        if result['type'] == 'query':
                            sql_result_message += f"查询成功，返回 {result['rowCount']} 条记录。"
                            logger.info(f"SQL查询成功，返回 {result['rowCount']} 条记录")
                        else:
                            sql_result_message += f"操作成功，影响了 {result['affected_rows']} 条记录。"
                            logger.info(f"SQL操作成功，影响了 {result['affected_rows']} 条记录")
                    else:
                        sql_result_message += f"执行失败：{result['error']}"
                        logger.error(f"SQL执行失败: {result['error']}")
                    
                    # 发送最终结果
                    await self.send(text_data=json.dumps({
                        'content': sql_result_message,
                        'data': result if result['status'] == 'success' else None,
                        'is_last_message': True
                    }))
                else:
                    logger.warning("无法生成有效的SQL语句")
                    await self.send(text_data=json.dumps({
                        'content': "无法生成有效的SQL语句，请尝试更清晰地描述您的需求。",
                        'error': "无法生成SQL",
                        'is_last_message': True
                    }))
            
        except Exception as e:
            # 发送错误消息
            error_message = f"处理消息时出错: {str(e)}"
            logger.exception(error_message)
            await self.send(text_data=json.dumps({
                'error': error_message,
                'is_last_message': True
            }))

    async def optimize_query_result(self, user_input: str, sql: str, result: Dict[str, Any], operation_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        使用AI优化查询结果的展示形式
        
        Args:
            user_input: 用户原始输入
            sql: 执行的SQL语句
            result: 查询结果
            operation_info: 操作信息
            
        Returns:
            Dict[str, Any]: 包含优化后展示形式的字典
        """
        try:
            # 提取前5条记录作为样本数据
            sample_data = result['results'][:min(5, len(result['results']))]
            
            # 创建简洁样本，避免过大的数据量
            sample_json = json.dumps(sample_data, ensure_ascii=False)
            
            # 计算完整记录数
            total_records = len(result['results'])
            
            # 构建系统提示词
            system_prompt = f"""
            你是一位专业的数据分析专家，负责将SQL查询结果以用户友好的方式展示出来。
            
            <用户问题>
            {user_input}
            </用户问题>
            
            <查询意图>
            {operation_info.get('description', '未提供')}
            </查询意图>
            
            <执行的SQL>
            {sql}
            </执行的SQL>
            
            <查询结果结构>
            样本数据（共{total_records}条记录）:
            {sample_json}
            </查询结果结构>
            
            <任务>
            请分析上述信息，提供两种格式的结果展示:
            1. 表格展示优化：为表格提供更具可读性的列名和展示结构
            2. 结果摘要：使用Markdown生成对结果的简明解释
            </任务>
            
            <输出规范>
            请严格按照以下JSON Schema格式返回结果:
            ```json-schema
            {{
              "type": "object",
              "required": ["table_display", "summary"],
              "properties": {{
                "table_display": {{
                  "type": "object",
                  "required": ["columns", "description"],
                  "properties": {{
                    "columns": {{
                      "type": "object",
                      "description": "列名映射表，key为原始字段名，value为优化后的列名"
                    }},
                    "description": {{
                      "type": "string",
                      "description": "对表格数据的简短描述"
                    }}
                  }}
                }},
                "summary": {{
                  "type": "string",
                  "description": "使用Markdown格式对查询结果的总结说明"
                }}
              }}
            }}
            ```
            
            <重要说明>
            1. 确保JSON格式严格有效，以便直接被解析使用
            2. 列名映射应覆盖所有原始字段，确保不漏不错
            3. 优化后的列名应简洁、直观、专业，并富有语义化
            4. summary必须使用中文，使用Markdown格式，包含重点数据分析和发现
            5. 如果数据量很大，你的分析只能基于提供的样本，请在summary中注明
            6. 表格列名必须简洁易读，避免过长，通常5-10个字为宜
            7. 你的输出不会直接展示给用户，而是经过系统处理后再呈现，所以请专注于提供高质量的格式化内容
            </重要说明>
            """
            
            # 调用AI进行优化
            reasoning, response = await chat(
                f"分析并优化查询结果展示：共{total_records}条数据",
                system_prompt
            )
            
            # 从AI响应中提取JSON
            json_pattern = r'```(?:json)?\s*({{.*?}})\s*```'
            json_match = re.search(json_pattern, response, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(1)
                formatted_result = json.loads(json_str)
                logger.info("成功优化查询结果展示")
                return formatted_result
            else:
                # 尝试直接解析整个响应为JSON
                try:
                    # 去除可能的前缀和后缀文本，只保留JSON部分
                    json_str = re.sub(r'^[^{]*({.*})[^}]*$', r'\1', response.strip(), flags=re.DOTALL)
                    formatted_result = json.loads(json_str)
                    return formatted_result
                except:
                    logger.warning("无法从AI响应中提取格式化结果，使用默认展示")
                    return {
                        "table_display": {
                            "columns": {},  # 空映射表示使用原始列名
                            "description": "查询结果"
                        },
                        "summary": f"查询返回了{total_records}条记录。"
                    }
        except Exception as e:
            logger.error(f"优化查询结果展示时出错: {str(e)}")
            return {
                "table_display": {
                    "columns": {},
                    "description": "查询结果"
                },
                "summary": f"查询返回了{total_records}条记录。"
            }

    def send_progress_message(self, message: str) -> None:
        """
        发送进度消息到前端
        
        Args:
            message: 进度消息内容
        """
        
        async_to_sync(self.send)(text_data=json.dumps({
            'content': '',
            'loading_message': message
        })) 

    def _get_relevant_tables(self, operation_type: str, description: str, db_structure: Dict[str, Dict[str, Any]]) -> str:
        """
        获取与操作相关的表推荐
        
        Args:
            operation_type: 操作类型
            description: 操作描述
            db_structure: 数据库结构
            
        Returns:
            str: 相关表推荐文本
        """
        relevant_tables = {}
        description_lower = description.lower()
        
        # 获取描述中的关键词
        words = re.findall(r'\b\w+\b', description_lower)
        word_set = set(words)
        
        for table_name, table_info in db_structure.items():
            table_score = 0
            matched_fields = []
            
            # 检查表名是否直接出现在描述中
            if table_name.lower() in description_lower or table_info['comment'].lower() in description_lower:
                table_score += 5
                matched_fields.append(f"{table_name}表(直接匹配)")
            
            # 检查字段匹配
            for col in table_info['columns']:
                col_name = col['name'].lower()
                col_comment = col['comment'].lower() if col['comment'] else ""
                
                # 字段名完全匹配
                if col_name in word_set:
                    table_score += 3
                    matched_fields.append(f"字段: {col['name']}")
                # 字段注释匹配
                elif col_comment and any(word in col_comment for word in words):
                    table_score += 2
                    matched_fields.append(f"字段说明: {col['comment']}")
            
            # 根据操作类型进一步调整相关性
            if operation_type == '查询' and ('id' in [col['name'].lower() for col in table_info['columns']]):
                table_score += 1  # 查询操作倾向于有id字段的表
                
            if operation_type == '新增' and any(col['name'].lower() == 'createtime' for col in table_info['columns']):
                table_score += 1  # 新增操作倾向于有createtime字段的表
            
            if table_score > 0:
                relevant_tables[table_name] = {
                    'score': table_score,
                    'comment': table_info['comment'],
                    'matched_fields': matched_fields[:3]  # 最多显示3个匹配的字段
                }
        
        # 按相关性分数排序
        sorted_tables = sorted(relevant_tables.items(), key=lambda x: x[1]['score'], reverse=True)
        
        # 构建输出文本
        if sorted_tables:
            result = []
            for table_name, info in sorted_tables[:5]:  # 最多显示前5个相关表
                matched = ", ".join(info['matched_fields'])
                result.append(f"- **{table_name}** ({info['comment']})\n  匹配点: {matched}")
            return "\n".join(result)
        else:
            return "无明确匹配的表，请根据数据库结构选择合适的表。" 
