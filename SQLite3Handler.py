import sqlite3
from logging import getLogger, Handler, LogRecord
import time
import datetime as dt
import traceback
from typing import Callable, TypeVar
# import pickle
# from copy import copy

SQLite3Available = TypeVar('SQLite3 Available')


class SQLite3Handler(Handler):
    "Logging Handler which inserts logs into sqlite3 database"
    
    class LogCol:
        "represents 1 column of table"
        def __init__(self, name: str,
                     get_value_func: Callable[[LogRecord], SQLite3Available],
                     col_type: str = 'TEXT'):
            self.name = name
            self.type = col_type
            self.get_value_func = get_value_func
        
        def get_value(self, record :LogRecord) -> SQLite3Available:
            """
            get value from a LogRecord object
            ---
            Parameters:
                record: logging.LogRecord
            ---
            Returns:
                data to be inserted into SQLite3 database
            """
            return self.get_value_func(record)

    TABLE_NAME = 'logs'

    TABLE_COLUMNS = (
        LogCol('Time', lambda x: dt.datetime(*time.localtime(x.created)[:6], int(x.msecs * 1000))),
        LogCol('LoggerName', lambda x: x.name),
        LogCol('Level', lambda x: x.levelname),
        LogCol('FileName', lambda x: x.filename),
        LogCol('LineNo', lambda x: x.lineno, 'INTEGER'),
        LogCol('ModuleName', lambda x: x.module),
        LogCol('FuncName', lambda x: x.funcName),
        LogCol('ProcessID', lambda x: x.process, 'INTEGER'),
        LogCol('ProcessName', lambda x: x.processName),
        LogCol('ThreadID', lambda x: x.thread, 'INTEGER'),
        LogCol('ThreadName', lambda x: x.threadName),
        LogCol('LogMessage', lambda x: x.getMessage()),
        LogCol('ExceptionType', lambda x: x.exc_info[0].__name__ if x.exc_info else None),
        LogCol('TraceBack', lambda x: ''.join(traceback.format_exception(*x.exc_info)) if x.exc_info else None)
    )
    
    def __init__(self, database: str, level: str = 'INFO'):
        self.database = database
        connection = sqlite3.connect(self.database)
        self.create_table(connection)
        connection.close()
        
        super().__init__(level)
        
    def emit(self, record: LogRecord):
        "this method is called when Logging-Event is triggered"
        connection = sqlite3.connect(self.database)
        try:
            self.insert_log(connection, record)
        except Exception:
            self.handleError(record)
        finally:
            connection.close()

    def insert_log(self, connection: sqlite3.Connection, record: LogRecord):
        "insert log into TABLE_NAME table"
        names = []
        values = []
        for col in self.TABLE_COLUMNS:
            names.append(col.name)
            values.append(col.get_value(record))
        cursor = connection.cursor()
        cursor.execute(f"""
            INSERT INTO {self.TABLE_NAME}({','.join(names)})
            VALUES({', '.join(['?'] * len(values))});
        """, values)
        connection.commit()
        
    def create_table(self, connection: sqlite3.Connection):
        "create TABLE_NAME table if it does not exist"
        cursor = connection.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS 
            {self.TABLE_NAME}(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {','.join(map(lambda x: f'{x.name} {x.type}', self.TABLE_COLUMNS))}
            );
        """)
        connection.commit()

    # Abolished methods

    # def create_debug_table(self, connection):
    #     "create 'raw_LogRecords' table"
    #     cursor = connection.cursor()
    #     cursor.execute('PRAGMA foreign_keys=true;')
    #     cursor.execute(f"""
    #         CREATE TABLE IF NOT EXISTS 
    #         raw_LogRecords(
    #             id INTEGER PRIMARY KEY,
    #             record NONE,
    #             foreign key (id) references {self.TABLE_NAME} (id)
    #         );
    #     """)
    #     connection.commit()
    
    # def insert_raw_LogRecord(self, connection, record):
    #     "insert into 'raw_LogRecords' table"
    #     cursor = connection.cursor()
    #     record = copy(record)
    #     if record.exc_info:
    #         record.exc_info = (
    #             record.exc_info[0],
    #             record.exc_info[1],
    #             "Because traceback object cannot be pickled, formatted string is stored instead of it. \n\n"
    #             + ''.join(traceback.format_exception(*record.exc_info))
    #         )
    #     pickled_record = pickle.dumps(record)
    #     idx = cursor.execute(f"SELECT MAX({self.TABLE_NAME}.id) FROM {self.TABLE_NAME}").fetchone()[0]
    #     cursor.execute("""
    #         INSERT INTO raw_LogRecords(id, record)
    #         VALUES (?, ?)
    #     """, [idx, pickled_record])
    #     connection.commit()
