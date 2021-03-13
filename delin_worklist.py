#  _____          _____           _           _____ ___  __  __
# |_   _|__  _   |_   _|__   ___ | |         |_   _/ _ \|  \/  |
#   | |/ _ \| | | || |/ _ \ / _ \| |  _____    | || | | | |\/| |
#   | | (_) | |_| || | (_) | (_) | | |_____|   | || |_| | |  | |
#   |_|\___/ \__, ||_|\___/ \___/|_|           |_| \___/|_|  |_|
#           | ___ /
#
# <プログラム名＞　
#  delin_worklist.py ：　勤務表登録プログラム
#  <概要＞
#  勤務表エクセルからデリートインサートを行うプログラム。
#  パラメータ指定フォルダ内にあるエクセルファイルをすべて登録します。
#  Excelは１シート目のみ登録。又、ユーザ名、年月をKEYとしたDelete後にINSERT処理をします。
#  テスト用に作成したものなので、入力チェックは行いませんので実行は自分の責任で。。。
#  SQL Exceptionが発生した場合は、近藤まで連絡。
#
#  created by BJProject(ichio.kondo)
#

import calendar
import glob
import MySQLdb
import openpyxl
import yaml

# 対象テーブル
tablename = 'WK_B_WORKIN_LIST'
# mysql接続情報
print( " ______  _____                ____       _____     ")
print( "/\__  _\/\  __`\  /'\_/`\    /\  _`\    /\___ \    ")
print( "\/_/\ \/\ \ \/\ \/\      \   \ \ \L\ \  \/__/\ \   ")
print( "   \ \ \ \ \ \ \ \ \ \__\ \   \ \  _ <'    _\ \ \  ")
print( "    \ \ \ \ \ \_\ \ \ \_/\ \   \ \ \L\ \__/\ \_\ \ ")
print( "     \ \_\ \ \_____\ \_\\ \_\   \ \____/\_\ \____/ ")
print( "      \/_/  \/_____/\/_/ \/_/    \/___/\/_/\/___/  ")
print( "                                                   ")
print( "                                                   ")
print( " 勤務表取込プログラムStart！！")

try:
    # config読み込み
    with open('config.yml') as file:
        yml = yaml.load(file)
    appid = "mysql"
    conn = MySQLdb.connect(
        unix_socket=yml[appid]["unix_socket"],
        user=yml[appid]["user"],
        passwd=yml[appid]["passwd"],
        host=yml[appid]["host"],
        db=yml[appid]["db"])

    # 　祝日チェック
    conn.autocommit(False)
    cur = conn.cursor()
    # 祝日マスター取得
    holiday_sql = "SELECT * FROM WK_M_HOLIDAY"
    cur.execute(holiday_sql)
    records = cur.fetchall()
    print("--Start--")
    files = glob.glob("./temp/*")

    for file in files:
        print(file + "　を読み込み実行します")
        wb = openpyxl.load_workbook(file, data_only=True)
        sheet = wb.worksheets[0]
        empNo = sheet['D1'].value  # 社員No
        empName = sheet['D2'].value  # 社員名
        year = sheet['A4'].value  # 年　A4
        month = sheet['D4'].value  # 月　D4
        _, lastday = calendar.monthrange(year, month)  # 月末日

        valulist = []
        for row in sheet["A10:H40"]:
            values = ['"' + str(empNo) + '"']
            colcount = 1  # 列数カウント用
            endflg = False
            for col in row:
                if colcount == 1:
                    dates = col.value
                    # 月の最終日だったらflgOn
                    if int(dates.strftime('%d')) == lastday:
                        endflg = True
                    datestr = dates.strftime('%Y/%m/%d')
                    w_list = ['月', '火', '水', '木', '金', '土', '日']
                    yobistr = w_list[dates.weekday()]
                    # 日曜日以外祝日チェック
                    if yobistr != '日':
                        for record in records:
                            
                            if record[0].strftime('%Y%m%d') == dates.strftime('%Y%m%d'):
                                yobistr = '祝'
                    values.append('"' + datestr + '"')
                    values.append('"' + yobistr + '"')
                elif colcount == 4:
                    if col.value is None:
                        holiday_kbn = ""
                    else:
                        holiday_kbn = col.value
                    values.append('"' + holiday_kbn + '"')
                elif colcount == 5 or colcount == 6:
                    times = col.value
                    if times is None:
                        values.append("0")
                    else:
                        timef = float(times.strftime('%H.%M'))
                        # 終了時間　9:00 〜　1:00 とかした場合の対応。　通常は２５：００と入れてもらう。
                        if times.strftime('%p') == "AM" and colcount == 6 and timef <= 5.00:
                           timef += 24.0
                        values.append(str(timef))
                elif colcount == 7:
                    if col.value is None:
                        values.append("0")
                    else:
                        values.append(str(col.value))
                elif colcount == 8:
                    if col.value is None:
                        values.append('""')
                    else:
                        values.append('"' + str(col.value) + '"')
                colcount += 1

            # YEARMONTHDAY,EMPLOYEE_NO,YOBI,START_TIME,END_TIME,BREAK_TIME,休日区分,休暇区分
            # 年月日,社員No,曜日,references,references,references,休日区分,休暇区分
            valustr = ""
            valustr = ",".join(values)
            valurecode = ""
            valurecode = "(" + valustr + ")"
            valulist.append(valurecode)
            _sql = "INSERT INTO {0}(EMPLOYEE_NO,YEARMONTHDAY,YOBI,休暇区分,START_TIME,END_TIME,BREAK_TIME,REMARKS) VALUES".format(
                tablename)
            if endflg:
                break
        # delete/insert実行
        stryearmonth = str(year) + str(month).zfill(2)
        sql = " DELETE FROM {0} WHERE DATE_FORMAT(YEARMONTHDAY, '%Y%m%') = {1} AND EMPLOYEE_NO = '{2}';" \
            .format(tablename, stryearmonth, empNo)
        cur.execute(sql)
        print(empName + ':' + str(year) + '/' + str(month).zfill(2) + '　を削除しました。')
        sql_insert = ",".join(valulist) + ';'
        _sql_insert = _sql + sql_insert
        print(_sql_insert)
        cur.execute((_sql_insert).encode("utf-8"))
        conn.commit()
        print(empName + ':' + str(year) + '/' + str(month).zfill(2) + '　を登録しました。')
except:
    conn.rollback()
    print(" Error発生　維持管理まで連絡")
    import traceback
    traceback.print_exc()
finally:
    cur.close
    conn.close
