import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import json
from typing import List, Dict, Optional


class ViolationDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_path)
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
        
        conn.commit()
        conn.close()
    
    def add_violation(self, group_id: str, user_id: str, user_name: str, 
                     forbidden_words: List[str], original_text: str, 
                     ban_duration: int, violation_count: int):
        """添加违规记录"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().date()
        
        cursor.execute('''
            INSERT INTO violations 
            (group_id, user_id, user_name, violation_count, forbidden_words, 
             original_text, ban_duration, last_violation_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            group_id, user_id, user_name, violation_count,
            json.dumps(forbidden_words, ensure_ascii=False),
            original_text[:500],
            ban_duration,
            str(today)
        ))
        
        conn.commit()
        conn.close()
    
    def get_user_violations(self, group_id: str, user_id: str) -> List[Dict]:
        """获取用户违规记录"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT violation_count, last_violation_date, ban_duration, 
                   forbidden_words, original_text, created_at
            FROM violations 
            WHERE group_id = ? AND user_id = ?
            ORDER BY last_violation_date DESC
        ''', (group_id, user_id))
        
        records = []
        for row in cursor.fetchall():
            records.append({
                'violation_count': row[0],
                'last_date': row[1],
                'ban_duration': row[2],
                'forbidden_words': json.loads(row[3]) if row[3] else [],
                'original_text': row[4],
                'created_at': row[5]
            })
        
        conn.close()
        return records
    
    def cleanup_old_records(self, max_days: int = 30):
        """清理过期记录"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_date = (datetime.now() - timedelta(days=max_days)).date()
        cursor.execute('DELETE FROM violations WHERE last_violation_date < ?', (str(cutoff_date),))
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        return deleted_count
