import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import json
from typing import List, Dict, Optional, Any
import threading
from contextlib import contextmanager


class DatabaseConnectionPool:
    """数据库连接池（优化版）"""
    
    def __init__(self, db_path: Path, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections: List[sqlite3.Connection] = []
        self.in_use: Set[sqlite3.Connection] = set()
        self.lock = threading.RLock()
        
        # 初始化连接池
        self._initialize_pool()
    
    def _initialize_pool(self) -> None:
        """初始化连接池"""
        with self.lock:
            for _ in range(min(5, self.max_connections)):  # 初始创建5个连接
                conn = self._create_connection()
                self.connections.append(conn)
    
    def _create_connection(self) -> sqlite3.Connection:
        """创建新的数据库连接"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')  # 启用WAL模式提高并发性能
        conn.execute('PRAGMA synchronous=NORMAL')  # 平衡性能和数据安全
        conn.execute('PRAGMA foreign_keys=ON')  # 启用外键约束
        return conn
    
    @contextmanager
    def get_connection(self):
        """
        获取数据库连接（上下文管理器）
        
        使用示例：
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(...)
        """
        conn = None
        try:
            conn = self._acquire_connection()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self._release_connection(conn)
    
    def _acquire_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        with self.lock:
            # 首先尝试复用空闲连接
            for conn in self.connections:
                if conn not in self.in_use:
                    self.in_use.add(conn)
                    return conn
            
            # 如果没有空闲连接且未达到上限，创建新连接
            if len(self.connections) < self.max_connections:
                conn = self._create_connection()
                self.connections.append(conn)
                self.in_use.add(conn)
                return conn
            
            # 所有连接都在使用中，等待（简单实现）
            import time
            for _ in range(10):  # 最多尝试10次
                time.sleep(0.1)
                for conn in self.connections:
                    if conn not in self.in_use:
                        self.in_use.add(conn)
                        return conn
            
            raise Exception("数据库连接池已满，请稍后重试")
    
    def _release_connection(self, conn: sqlite3.Connection) -> None:
        """释放数据库连接"""
        with self.lock:
            if conn in self.in_use:
                self.in_use.remove(conn)
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """执行查询并返回结果"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            results = []
            for row in cursor.fetchall():
                results.append(dict(row))
            
            return results
    
    def execute_update(self, query: str, params: tuple = ()) -> int:
        """执行更新操作并返回影响的行数"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """批量执行更新操作"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount
    
    def close_all(self) -> None:
        """关闭所有连接"""
        with self.lock:
            for conn in self.connections:
                try:
                    conn.close()
                except Exception as e:
                    print(f"关闭数据库连接失败: {e}")
            
            self.connections.clear()
            self.in_use.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取连接池统计信息"""
        with self.lock:
            return {
                "total_connections": len(self.connections),
                "in_use_connections": len(self.in_use),
                "max_connections": self.max_connections,
                "available_connections": len(self.connections) - len(self.in_use)
            }


class ViolationDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.pool = DatabaseConnectionPool(db_path)
        self.init_database()
    
    def init_database(self):
        """初始化数据库"""
        with self.pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # 创建违规记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS violations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    user_name TEXT,
                    violation_count INTEGER DEFAULT 1,
                    forbidden_words TEXT,
                    original_text TEXT,
                    ban_duration INTEGER,
                    last_violation_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_user ON violations(group_id, user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_last_date ON violations(last_violation_date)')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_group_user_date 
                ON violations(group_id, user_id, last_violation_date DESC)
            ''')
            
            conn.commit()
    
    def add_violation(self, group_id: str, user_id: str, user_name: str, 
                     forbidden_words: List[str], original_text: str, 
                     ban_duration: int, violation_count: int) -> bool:
        """添加违规记录"""
        try:
            today = datetime.now().date()
            
            query = '''
                INSERT INTO violations 
                (group_id, user_id, user_name, violation_count, forbidden_words, 
                 original_text, ban_duration, last_violation_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            params = (
                group_id, user_id, user_name, violation_count,
                json.dumps(forbidden_words, ensure_ascii=False),
                original_text[:500],
                ban_duration,
                str(today)
            )
            
            self.pool.execute_update(query, params)
            return True
            
        except Exception as e:
            print(f"添加违规记录失败：{e}")
            return False
    
    def get_user_violations(self, group_id: str, user_id: str) -> List[Dict]:
        """获取用户违规记录"""
        try:
            query = '''
                SELECT violation_count, last_violation_date, ban_duration, 
                       forbidden_words, original_text, created_at
                FROM violations 
                WHERE group_id = ? AND user_id = ?
                ORDER BY last_violation_date DESC
            '''
            
            results = self.pool.execute_query(query, (group_id, user_id))
            
            records = []
            for row in results:
                records.append({
                    'violation_count': row['violation_count'],
                    'last_date': row['last_violation_date'],
                    'ban_duration': row['ban_duration'],
                    'forbidden_words': json.loads(row['forbidden_words']) if row['forbidden_words'] else [],
                    'original_text': row['original_text'],
                    'created_at': row['created_at']
                })
            
            return records
            
        except Exception as e:
            print(f"获取用户违规记录失败：{e}")
            return []
    
    def get_group_violations(self, group_id: str, limit: int = 100) -> List[Dict]:
        """获取群组违规记录"""
        try:
            query = '''
                SELECT user_id, user_name, violation_count, last_violation_date, 
                       ban_duration, forbidden_words, original_text
                FROM violations 
                WHERE group_id = ?
                ORDER BY last_violation_date DESC, violation_count DESC
                LIMIT ?
            '''
            
            results = self.pool.execute_query(query, (group_id, limit))
            
            records = []
            for row in results:
                records.append({
                    'user_id': row['user_id'],
                    'user_name': row['user_name'],
                    'violation_count': row['violation_count'],
                    'last_date': row['last_violation_date'],
                    'ban_duration': row['ban_duration'],
                    'forbidden_words': json.loads(row['forbidden_words']) if row['forbidden_words'] else [],
                    'original_text': row['original_text']
                })
            
            return records
            
        except Exception as e:
            print(f"获取群组违规记录失败：{e}")
            return []
    
    def cleanup_old_records(self, max_days: int = 30) -> int:
        """清理过期记录"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=max_days)).date()
            
            query = 'DELETE FROM violations WHERE last_violation_date < ?'
            
            deleted_count = self.pool.execute_update(query, (str(cutoff_date),))
            return deleted_count
            
        except Exception as e:
            print(f"清理过期记录失败：{e}")
            return 0
    
    def reset_user_violations(self, group_id: str, user_id: str) -> int:
        """重置用户违规记录"""
        try:
            query = 'DELETE FROM violations WHERE group_id = ? AND user_id = ?'
            
            deleted_count = self.pool.execute_update(query, (group_id, user_id))
            return deleted_count
            
        except Exception as e:
            print(f"重置用户违规记录失败：{e}")
            return 0
    
    def get_violation_stats(self, group_id: Optional[str] = None) -> Dict[str, Any]:
        """获取违规统计信息"""
        try:
            stats = {
                'total_violations': 0,
                'unique_users': 0,
                'total_bans': 0,
                'by_date': {},
                'top_users': []
            }
            
            if group_id:
                # 获取特定群组的统计
                query_total = '''
                    SELECT COUNT(*) as total, 
                           COUNT(DISTINCT user_id) as unique_users,
                           SUM(CASE WHEN ban_duration > 0 THEN 1 ELSE 0 END) as total_bans
                    FROM violations 
                    WHERE group_id = ?
                '''
                results = self.pool.execute_query(query_total, (group_id,))
                
                if results:
                    stats['total_violations'] = results[0]['total'] or 0
                    stats['unique_users'] = results[0]['unique_users'] or 0
                    stats['total_bans'] = results[0]['total_bans'] or 0
                
                # 获取按日期统计
                query_by_date = '''
                    SELECT last_violation_date, COUNT(*) as count
                    FROM violations 
                    WHERE group_id = ?
                    GROUP BY last_violation_date
                    ORDER BY last_violation_date DESC
                    LIMIT 30
                '''
                date_results = self.pool.execute_query(query_by_date, (group_id,))
                
                for row in date_results:
                    stats['by_date'][row['last_violation_date']] = row['count']
                
                # 获取违规最多的用户
                query_top_users = '''
                    SELECT user_id, user_name, COUNT(*) as violation_count,
                           MAX(last_violation_date) as last_violation
                    FROM violations 
                    WHERE group_id = ?
                    GROUP BY user_id, user_name
                    ORDER BY violation_count DESC
                    LIMIT 10
                '''
                user_results = self.pool.execute_query(query_top_users, (group_id,))
                
                for row in user_results:
                    stats['top_users'].append({
                        'user_id': row['user_id'],
                        'user_name': row['user_name'],
                        'violation_count': row['violation_count'],
                        'last_violation': row['last_violation']
                    })
            else:
                # 获取全局统计
                query_total = '''
                    SELECT COUNT(*) as total, 
                           COUNT(DISTINCT user_id) as unique_users,
                           SUM(CASE WHEN ban_duration > 0 THEN 1 ELSE 0 END) as total_bans
                    FROM violations
                '''
                results = self.pool.execute_query(query_total)
                
                if results:
                    stats['total_violations'] = results[0]['total'] or 0
                    stats['unique_users'] = results[0]['unique_users'] or 0
                    stats['total_bans'] = results[0]['total_bans'] or 0
            
            return stats
            
        except Exception as e:
            print(f"获取违规统计失败：{e}")
            return {}
    
    def close(self):
        """关闭数据库连接池"""
        if hasattr(self, 'pool'):
            self.pool.close_all()
