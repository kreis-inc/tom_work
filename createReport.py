#  _____          _____           _           _____ ___  __  __
# |_   _|__  _   |_   _|__   ___ | |         |_   _/ _ \|  \/  |
#   | |/ _ \| | | || |/ _ \ / _ \| |  _____    | || | | | |\/| |
#   | | (_) | |_| || | (_) | (_) | | |_____|   | || |_| | |  | |
#   |_|\___/ \__, ||_|\___/ \___/|_|           |_| \___/|_|  |_|
#           | ___ /
#
# <プログラム名＞　
#  createReport.py ： 勤務集計結果をExcelへ出力
# <概要＞
# 勤務集計結果をExcelへ出力するプログラム
# 月次の締処理後に実行を行う。WK_B_PROCESSの最終締月の出力を行う。
# 出力Excelのテンプレートはformatフォルダにある事が前提。
# 1. 勤務表集計一覧　 'yyyy_mm＿勤務集計一覧' でファイル作成
# 2. ユーザ別集計結果表（勤務表） 'yyyy_mm_氏名_勤務表' でファイル作成
# 3. Slackへ送信
#
import os
import datetime
import shutil
import openpyxl
import MySQLdb

# 0 実行年月取得
import yaml


def getExcecDate( _cur ):
    date_sql = "select max(YEARMONTH) from WK_B_PROCESS"
    cur.execute(date_sql)
    execDateRecordes = _cur.fetchall()
    for record in execDateRecordes:
        return record

def getDetail(employee_no, yearmonth):
    sql = " select " \
          "l.EMPLOYEE_NO, l.YEARMONTHDAY, c.YOBI, c.PAID, l.START_TIME, l.END_TIME, l.BREAK_TIME, c.SUMTIME, c.REMARKS " \
          "from WK_B_WORKIN_LIST l LEFT OUTER JOIN WK_B_WORKIN_LIST_CONFIRM c ON l.EMPLOYEE_NO = c.EMPLOYEE_NO AND l.YEARMONTHDAY = c.YEARMONTHDAY " \
          "WHERE l.EMPLOYEE_NO = {0} AND DATE_FORMAT(l.YEARMONTHDAY, '%Y%m') = {1};".format(employee_no, yearmonth)
    return sql

#　確定勤務表テンプレートファイルのコピー
def copyTemplate(copyFile,yearmonth):
    # カレントディレクトリ取得
    dirname = os.getcwd()
    # now_datetime = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    # テンプレートファイルのcopy及び請求書excelの作成
    copy = os.path.join(dirname, yearmonth + '_勤務表集計一覧.xlsx')
    shutil.copyfile(copyFile, copy)
    return copy

try:
    with open('config.yml') as file:
        yml = yaml.load(file)
    appid = "mysql"
    conn = MySQLdb.connect(
        unix_socket=yml[appid]["unix_socket"],
        user=yml[appid]["user"],
        passwd=yml[appid]["passwd"],
        host=yml[appid]["host"],
        db=yml[appid]["db"])

    conn.autocommit(False)
    cur = conn.cursor()
    # 締処理テーブルより最新の締月を取得
    execDate = getExcecDate(cur)
    yearmonth = str(execDate[0])

    #　集計一覧のテンプレート場所。
    TEMP_SUMLIST = "tom/working/format/テンプレート_勤怠表集計一覧.xlsx"

    # 集計一覧の作成ファル名　xxxx年xx月_勤務表集計一覧.xlsx
    copy = copyTemplate(TEMP_SUMLIST,yearmonth)
    # 新規シート作成
    invoice_wb = openpyxl.load_workbook(copy)
    invoice_ws = invoice_wb.worksheets[1]
    #invoice_cp_ws = invoice_wb.copy_worksheet(invoice_ws)
    #invoice_cp_ws.title = tri_name + "_" + order_no¥
    # WK_B_WOKING_SUMの指定月でList取得。
except:
    conn.rollback()
    print(" 異常終了しました。維持管理まで連絡")
    import traceback

    traceback.print_exc()
finally:
    cur.close
    conn.close