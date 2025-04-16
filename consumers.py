# 标准库导入
import json
import logging
import re
from typing import Dict, Any, Optional, Tuple

# 第三方库导入
from asgiref.sync import sync_to_async
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
        同步执行SQL语句的辅助方法
        
        Args:
            sql: 要执行的SQL语句
            
        Returns:
            Dict[str, Any]: 包含执行结果或错误信息的字典
        """
        try:
            # 使用Django的数据库连接执行SQL
            with connection.cursor() as cursor:
                cursor.execute(sql)
                
                try:
                    # 尝试获取结果
                    rows = cursor.fetchall()
                    # 获取列名
                    columns = [col[0] for col in cursor.description]
                    
                    # 构建结果
                    results = []
                    for row in rows:
                        result_dict = {}
                        for i, value in enumerate(row):
                            # 处理datetime和date类型，转换为字符串
                            if hasattr(value, 'isoformat'):  # 检查是否为datetime或date类型
                                result_dict[columns[i]] = value.isoformat()
                            # 处理其他不可JSON序列化的类型
                            elif isinstance(value, (set, frozenset)):
                                result_dict[columns[i]] = list(value)
                            elif value is None or isinstance(value, (str, int, float, bool, list, dict)):
                                result_dict[columns[i]] = value
                            else:
                                # 其他类型转换为字符串
                                result_dict[columns[i]] = str(value)
                        results.append(result_dict)
                    
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
        # 构建提示词，判断用户是否想执行数据操作
        system_prompt = """
        你是一个数据库助手，你当前负责与用户的对话聊天 及 数据库操作。
        你的任务是分析用户的输入，判断他们是否想要执行数据库操作。
        
        你拥有的特殊能力：
        1. 你可以根据用户的需求，生成对应的SQL语句。
        
        
        如果用户想执行数据库操作，请按照以下格式输出：
        - 对于新增操作，输出: ```新增 + 对SQL操作的简洁描述```
        - 对于查询操作，输出: ```查询 + 对SQL操作的简洁描述```
        - 对于更改操作，输出: ```更改 + 对SQL操作的简洁描述```
        - 对于删除操作，输出: ```删除 + 对SQL操作的简洁描述```
        
        例如，当用户询问"查询当前的用户有哪些"时，你可以回复：
        "我可以帮你查询当前的用户有哪些。以下是查询语句："

        ```查询
        查询全部用户
        ```
        
        注意：
        1. 如果用户的输入不是关于数据库操作，或者不清楚具体意图，只需正常回复用户，不要使用以上特殊格式。
        2. 你可以使用markdown的格式输出
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
        你是一个SQL生成助手。根据用户的描述，生成对应的SQL语句。

        以下是数据库结构概览：
        {formatted_db_structure}

        请按以下要求生成SQL:
        1. 只输出SQL语句，不要有任何解释
        2. 使用markdown的SQL代码块格式输出，即 ```sql 开始，``` 结束
        3. 生成的SQL必须是完整、有效、可直接执行的
        4. 不要使用不存在的表或字段
        5. 确保SQL语法正确
        6. 如果用户想要查询，请使用select语句，如果用户想要新增，请使用insert into语句，如果用户想要更改，请使用update语句，如果用户想要删除，请使用delete语句
        """
        
        user_prompt = f"操作类型: {operation_type}\n描述: {description}"
        reasoning, response = await chat(user_prompt, system_prompt)
        
        # 从响应中提取SQL语句
        sql = await self.extract_sql_commands(response)
        return sql, response

    async def receive(self, text_data: str) -> None:
        """
        接收WebSocket消息，适配前端AiChat.vue
        
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
                'loading_message': '⏳ 正在分析您的请求...'
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
                db_structure_text = await self.format_db_structure_for_ai(db_structure)
                
                await self.send(text_data=json.dumps({
                    'content': f"\n\n### 数据库结构概览\n{db_structure_text}",
                    'is_last_message': False
                }))
                
                await self.send(text_data=json.dumps({
                    'content': '',
                    'loading_message': f"⏳ 正在生成{operation_info['operation_type']}操作的SQL语句..."
                }))
                
                # 生成SQL
                sql, sql_response = await self.generate_sql(operation_info)
                
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
                        'loading_message': "⏳ 正在执行SQL..."
                    }))
                    
                    # 执行SQL并获取结果
                    result = await self.execute_sql(sql)
                    
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