# reads the json
# gets names and emails from database
# reads emails from file
# prints output log.
# example: ```[QW-NWM-1-001214][28DAT17][leah@leahdavidsonlifecoaching.com][HJAV]_000029916.pdf```


import csv
import urllib.parse

from typing import TypedDict
import json
import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
import shutil
import openpyxl

SQL_USERNAME = os.environ['SQL_USERNAME']
SQL_PASSWORD = urllib.parse.quote_plus(os.environ['SQL_PASSWORD'])
SQL_DATABASE = os.environ['SQL_DATABASE']

class EntryMap(TypedDict):
    BKR_ID: str
    GROUP_OFFSET: str
    GROUP_LENGTH: str
    COR_CPY_CDE: str
    CPY_NUM: str
    COR_BKR_ID: str
    REP_ID: str
    BRANCH_ID: str
    CLT_ID: str
    MAIN_CLT_ID: str
    ACT_ID: str
    STMT_DTE: str
    NO_PAGES: str
    COR_LNG_CDE: str
    RECEIPT_NUM: str
    SIN: str
    REPL_RUN_DTE: str
    IND_DOC_TYP: str
    HDR_DOC_TYP: str
    DOC_CMT: str
    FI_ID: str
    RUN_DTE: str
    FIRM_ID: str

class ExcelRow(TypedDict): #spaces insteead of '_'
    Account_ID: str
    Client_ID: str
    Email_Address: str
    Spouse_POA_ID: str

def read_only_json() -> dict[str, EntryMap]:
    json_files:list[str] = [file for file in  os.listdir() if file.endswith('.json')]
    if len(json_files) != 1:
        raise ValueError("Error: There should be exactly one JSON file in the directory.")
    with open(json_files[0], 'r') as f:
        data = json.load(f)
        return data

def get_info_from_sql(account_number_array:list[str]) -> list[tuple[str, str, str]]:
    connection_url = f'mysql+pymysql://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_DATABASE}.criwycoituxs.ca-central-1.rds.amazonaws.com/qw_prod'
    engine = create_engine(connection_url)
    sql_array = "'" + "','".join(account_number_array) + "'"
    sql_query = text(f"""
    SELECT
        a.accountNumber,
        a.accountOwnersQID,
        u.email
    FROM
        account AS a 
    LEFT JOIN
        users AS u
    ON
        a.accountOwnersQID = u.QID 
        AND u.currentFlag = 1 
        AND u.deletedFlag = 0
    WHERE
        a.accountNumber IN ({sql_array}) 
        AND a.currentFlag = 1 
        AND a.deletedFlag = 0 
    """)
    db_session = scoped_session(sessionmaker(bind=engine))
    result_proxy = db_session.execute(sql_query).fetchall()
    return [x for x in result_proxy]

def find_account_record_from_sql(accounts, account_to_find:str) -> tuple[str, str, str]:
    for account in accounts:
        if account[0] == account_to_find:
            return account
    return tuple("", "", "")

def find_account_from_xlsx(xlsx:list[ExcelRow], account_to_find:str) -> ExcelRow:
    for entries in xlsx:
        if entries['Account ID'] == account_to_find:
            return entries
    return {"Account ID": account_number, "Client ID": "", "Email Address": "", "Spouse POA ID": ""}

def into_from_xlsx() -> list[ExcelRow]:
    workbook = openpyxl.load_workbook("../Document Naming for Matt.xlsx")
    sheet = workbook.worksheets[0]
    headers = [cell.value for cell in sheet[1]]
    data = []
    for row in sheet.iter_rows(min_row=2, values_only=True):
        row_dict = {}
        for header, value in zip(headers, row):
            row_dict[header] = value
        data.append(row_dict)
    return data

def make_output_directory(directory_name='output'):
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

def write_json_to_file(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
        
def write_files_to_csv(filename: str, data:list[list[str]]):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            writer.writerow(row)
        
def get_parent_directory() -> str:
    current_directory = os.getcwd()
    return os.path.basename(current_directory)

if __name__ == "__main__":
    directory_name: str = f"{get_parent_directory()}_output"
    make_output_directory(directory_name)
    file_map: dict[str, EntryMap] = read_only_json()
    bank_accounts: list[str] = [str(x['ACT_ID']) for x in file_map.values()]
    sqls = get_info_from_sql(bank_accounts)
    xlsxes = into_from_xlsx()
    
    cwd = os.getcwd()
    things_that_are_bad = {}
    filenames = [
        ['new filename', 'qid', 'account number', 'email', 'rep id', 'incoming filename', '', '', 'excel qid', 'sql qid', 'excel email', 'excel sql']
    ]
    for filename, file_info in file_map.items():
        account_number:str = file_info['ACT_ID']
        record_sql:str = find_account_record_from_sql(sqls, account_number)
        record_xlsx: tuple[str,str,str] = find_account_from_xlsx(xlsxes, account_number)
        
        qid_from_xlsx: str = record_xlsx["Client ID"].upper()
        qid_from_sql: str = str(record_sql[1]).upper()
        
        email_from_xlsx:str = record_xlsx["Email Address"].lower()
        email_from_sql: str = str(record_sql[2]).lower()

        if qid_from_xlsx != qid_from_sql or qid_from_xlsx == "":
            things_that_are_bad[account_number] = {}
            things_that_are_bad[account_number].update({'qid_from_xlsx': qid_from_xlsx, 'qid_from_sql':qid_from_sql})
        if email_from_xlsx != email_from_sql or email_from_xlsx == "":
            if account_number not in things_that_are_bad:
                things_that_are_bad[account_number] = {}
            things_that_are_bad[account_number].update({'email_from_xlsx': email_from_xlsx, 'email_from_sql':email_from_sql})

        qid = qid_from_xlsx if qid_from_xlsx != "" else qid_from_sql
        email = email_from_xlsx if email_from_xlsx != "" else email_from_sql
                        
        new_filename = f"[{qid}][{account_number}]{email}[{file_info['REP_ID']}]_{filename}"
        shutil.copy(
            os.path.join(cwd, filename),
            os.path.join(cwd, directory_name, new_filename)
        )
        filenames += [[new_filename, qid, account_number, email, file_info['REP_ID'], filename, '' , '' ,qid_from_xlsx, qid_from_sql, email_from_xlsx, email_from_sql]]
    
    print(json.dumps(things_that_are_bad, indent=2))
    write_json_to_file(things_that_are_bad, os.path.join(cwd,directory_name,f'things_that_are_bad.json'))
    write_files_to_csv(os.path.join(cwd, directory_name, f'files_in_{get_parent_directory()}.csv'), filenames)