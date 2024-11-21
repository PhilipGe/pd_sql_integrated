from databases import Database
import numpy as np
import pandas as pd
import asyncio
import shutil
import os

def CREATE_COLUMNLIST_statement(pd_df: pd.DataFrame):
    pandas_to_sql_dtype_map = {
        'int64': 'BIGINT',
        'int32': 'INT',
        'float64': 'DOUBLE',
        'float32': 'FLOAT',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
    }

    column_str = ""
    for name, type in pd_df.dtypes.items():
        column_str += f"{name} {pandas_to_sql_dtype_map.get(str(type), "TEXT")}, "

    return f"{column_str.rstrip(", ")}"

def JUST_COLUMNLIST_statement(pd_df: pd.DataFrame, prefix=""):
    column_str = ""
    for name, type in pd_df.dtypes.items():
        column_str += f"{prefix}{name}, "

    return f"{column_str.rstrip(", ")}"

def CREATE_TABLE_statement(tablename: str, pd_df: pd.DataFrame):
    return f"CREATE TABLE {tablename} ({CREATE_COLUMNLIST_statement(pd_df)}, UNIQUE ({JUST_COLUMNLIST_statement(pd_df)}));"

def INSERT_INTO_TABLE_statement(tablename: str, pd_df: pd.DataFrame):
    return f"INSERT INTO {tablename} ({JUST_COLUMNLIST_statement(pd_df)}) VALUES ({JUST_COLUMNLIST_statement(pd_df, ":")}) ON CONFLICT ({JUST_COLUMNLIST_statement(pd_df)}) DO NOTHING;"

def GET_ALL_TABLES_statement():
    return  """SELECT name
    FROM sqlite_master
    WHERE type='table';"""

def GET_ALL_RECORDS_statement(tablename):
    return f"""SELECT * FROM {tablename};"""

class PandasSQLInterface:

    def __init__(self, database):
        self.db = Database(database)
    
    async def connect(self):
        await self.db.connect()

    async def disconnect(self):
        await self.db.disconnect()

    async def create_table(self, tablename, pd_df: pd.DataFrame):
        query = CREATE_TABLE_statement(tablename, pd_df); # print(query)
        await self.db.execute(query)
        for record in pd_df.to_dict(orient="records"):
            await self.save_record_to_table(tablename, pd_df, record)

    async def save_record_to_table(self, tablename, pd_df_schema: pd.DataFrame, record):
        query = INSERT_INTO_TABLE_statement(tablename, pd_df_schema); # print(query)
        await self.db.execute(query, record)

    async def get_table_into_datafame(self, tablename) -> pd.DataFrame:
        query = f"""
        SELECT *
        FROM {tablename};
        """
        result = await self.db.fetch_all(query)

        data = map(lambda x: dict(x), result)

        df = pd.DataFrame(data)

        print(df)

        return df

    async def dump_to_csv(self, folder, overwrite = False):
        if(os.path.exists(folder)): 
            if(not overwrite): raise ValueError(f"Folder {folder} already exists. Either set overwrite flag or create a new folder name")
            else: shutil.rmtree(folder)
        
        os.mkdir(folder)
        
        query = GET_ALL_TABLES_statement(); # print(query)
        results = await self.db.fetch_all(query)
        tablenames = [row["name"] for row in results]

        for name in tablenames:
            temp_df = await self.get_table_into_datafame(name)
            temp_df.to_csv(f"{folder}/{name}.csv")

async def initialize_db(dblink: str, tablenames: list[str], tableschemas: list[pd.DataFrame]):
    interface = DbInterface(dblink)
    await interface.connect()
    for i in range(len(tablenames)):
        await interface.create_table(tablenames[i], tableschemas[i])
    await interface.disconnect()

async def dump_db_to_folder(dblink, folder, overwrite=True):
    interface = DbInterface(dblink)
    await interface.connect()
    await interface.dump_to_csv(folder, overwrite=overwrite)
    await interface.disconnect()

async def helloworld(url, dumpfolder):
    interface = DbInterface(url)

    df = pd.DataFrame({
        "Col1": ["Hello", "World"],
        "Col2": [6.0,3.0]
    })

    await interface.connect()
    await interface.create_table("HelloWorldTable", df)
    await interface.save_record_to_table("HelloWorldTable", df, {"Col1":"Goodbye","Col2":-5.0})
    await interface.dump_to_csv(dumpfolder, overwrite=False)
    await interface.disconnect()

if __name__ == "__main__":
    DATABASE_LINK = "sqlite:///helloworld.db"
    DUMPFOLDER = "HelloWorldFolder"

    # Example One:
    # asyncio.run(helloworld(DATABASE_LINK, DUMPFOLDER))

    # Example Two:
    # COLUMNS = {
    #     "Col1": str,
    #     "Col2": np.float64
    # }
    # TABLE_SCHEMA = pd.DataFrame(columns={col: pd.Series(dtype=dtype) for col, dtype in COLUMNS.items()})
    # TABLE_NAME = "HelloWorldTable"
    # asyncio.run(initialize_db(DATABASE_LINK,[TABLE_NAME],[TABLE_SCHEMA]))
    # asyncio.run(dump_db_to_folder(DATABASE_LINK,DUMPFOLDER))
