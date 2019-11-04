import sqlite3
from logging import getLogger, Handler, LogRecord
import time
import datetime as dt
import traceback
import re
from typing import Callable, Union

__all__ = ['SQLite3Handler', 'TimedRotatingSQLite3Handler']


class SQLite3Handler(Handler):
    "Logging Handler which inserts logs into sqlite3 database"

    class LogCol:
        "represents 1 column of table"

        def __init__(self, name: str,
                     get_value_func: Callable[[LogRecord], Union[int, float, str, dt.datetime]],
                     col_type: str = 'TEXT'):
            self.name = name
            self.type = col_type
            self.get_value_func = get_value_func

        def get_value(self, record: LogRecord) -> Union[int, float, str, dt.datetime]:
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
        LogCol('Time', lambda x: dt.datetime(
            *time.localtime(x.created)[:6], int(x.msecs * 1000))),
        LogCol('LoggerName', lambda x: x.name),
        LogCol('Level', lambda x: x.levelname),
        LogCol('FileName', lambda x: x.pathname),
        LogCol('LineNo', lambda x: x.lineno, 'INTEGER'),
        LogCol('ModuleName', lambda x: x.module),
        LogCol('FuncName', lambda x: x.funcName),
        LogCol('ProcessID', lambda x: x.process, 'INTEGER'),
        LogCol('ProcessName', lambda x: x.processName),
        LogCol('ThreadID', lambda x: x.thread, 'INTEGER'),
        LogCol('ThreadName', lambda x: x.threadName),
        LogCol('LogMessage', lambda x: x.getMessage()),
        LogCol('ExceptionType',
               lambda x: x.exc_info[0].__name__ if x.exc_info else None),
        LogCol('TraceBack', lambda x: ''.join(
            traceback.format_exception(*x.exc_info)) if x.exc_info else None)
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


class TimedRotatingSQLite3Handler(SQLite3Handler):
    "Logging Handler which inserts logs into sqlite3 database"

    def __init__(self, database: str, interval: str, level: str = 'INFO'):
        self.interval = interval
        dbext_re = '(\.db$|\.sqlite$|\.sqlite3$)'
        dbname = re.sub(dbext_re, '', database)
        dbext = re.search(dbext_re, database)
        dbext = dbext.string[dbext.start(): dbext.end()
                             ] if dbext else '.sqlite3'
        if interval == 'year':
            timeformat = '_%Y'
        elif interval == 'month':
            timeformat = '_%Y-%m'
        elif interval == 'day':
            timeformat = '_%Y-%m-%d'
        elif interval == 'hour':
            timeformat = '_%Y-%m-%d_%H'
        elif interval == 'minute':
            timeformat = '_%Y-%m-%d_%H:%M'
        else:
            timeformat = ''
        self.dbformat = dbname + timeformat + dbext

        # Handler.__init__(self, level)
        super(SQLite3Handler, self).__init__(level)

    def emit(self, record: LogRecord):
        date = dt.datetime(*time.localtime(record.created)[:6])
        database = date.strftime(self.dbformat)
        connection = sqlite3.connect(database)
        try:
            self.create_table(connection)
            self.insert_log(connection, record)
        except Exception as e:
            self.handleError(record)
        finally:
            connection.close()
