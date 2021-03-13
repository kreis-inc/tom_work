#
#  _____          _____           _           _____ ___  __  __
# |_   _|__  _   |_   _|__   ___ | |         |_   _/ _ \|  \/  |
#   | |/ _ \| | | || |/ _ \ / _ \| |  _____    | || | | | |\/| |
#   | | (_) | |_| || | (_) | (_) | | |_____|   | || |_| | |  | |
#   |_|\___/ \__, ||_|\___/ \___/|_|           |_| \___/|_|  |_|
#            |___/
#
# <プログラム名＞　
#  月の基準時間算出プログラム(12ヶ月分作成）
#
# <概要>
#  クライス期初管理テーブル　WK_M_PERIOD
#  年月別労働基準時間管理テーブル WK_B_WORKINGTIME_BASE
#  上記２つのマスターを作成するバッチプログラム。
#  計算スタート年月より12ヶ月分の　月営業日数、月標準時間の計算を行いデータベースへ登録します。
#  祝日は祝日マスター(WK_M_HOLIDAY)より取得し算出。
# <注意＞
#  期は変数にて管理。period
#  登録するマスターはDeleteされない為手動で消してから実行する事。
#
# created by BJProject(ichio.kondo)
#

import MySQLdb
import datetime
import calendar
import yaml
from dateutil.relativedelta import relativedelta

# ------------- バッチ実行前に手動で変更する事 ---------------------
# Kreis期
period = 24
# 計算開始年月
start = '2020-07-01'
# 計算算出月数
numberOfTime = 12
# --------------------------------------------------------------

def isBizDay(Date, holidayList):
    # Date = datetime.date(int(DATE[0:4]), int(DATE[4:6]), int(DATE[6:8]))
    if iskreisHoliday(Date, holidayList):
        return 1
    elif Date.weekday() >= 5:
        return 1
    else:
        return 0

def iskreisHoliday(daTe, holidayList):
    for hd in holidayList:
        if hd == daTe:
            return True
    return False

# 計算スタート年月
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
    conn.autocommit(False)
    cur = conn.cursor()
    # 祝日マスター取得
    holiday_sql = 'SELECT * FROM WK_M_HOLIDAY'
    cur.execute(holiday_sql)
    records = cur.fetchall()
    holiday = []

    #祝日マスターをListへ
    for record in records:
        holiday.append(record[0])

    #start年月をDatetime
    tdatetime = datetime.datetime.strptime(start, '%Y-%m-%d')

    sum_fullDay = 0
    sum_dayCnt = 0
    sum_holCnt = 0
    sum_timeCnt = 0

    # 回数分ループ（１２ヶ月分）
    for i in range(numberOfTime):
        if i == 0:
            tdate = datetime.date(tdatetime.year, tdatetime.month, tdatetime.day)
        else:
            tdate = tdate + relativedelta(months=1)

        fullDay = 0  # 年月集計用
        dayCnt = 0  # 年月集計用
        holCnt = 0  # 年月集計用
        holList = []  # 年月の休日用List

        # 該当日数分ループし、営業日　or 休みかをカウント
        for ix in range(calendar.monthrange(tdate.year, tdate.month)[1]):
            d = datetime.datetime(tdate.year, tdate.month, (ix + 1)).date()
            fullDay += 1
            if isBizDay(d, holiday) == 0:
                dayCnt += 1
            else:
                holCnt += 1
                holList.append(d)

        #　年月の集計用デバッグ
        print("--------------------------------")
        print("年月 = " + str(tdate.year) + "/" + str(tdate.month).zfill(2))
        print("営業日数 = " + str(dayCnt))
        print("月基準時間 = " + str(dayCnt * 8))
        print("休日日数 = " + str(holCnt))
        print("総日数 = " + str(fullDay))

        #　データベースへ登録
        nengetu = str(tdate.year) + str(tdate.month).zfill(2)  # 年月↲
        #WK_B_WORKINGTIME_BASE
        workingtimeList = [str(nengetu),str(period),str(dayCnt),str(holCnt),str(dayCnt * 8)]
        sql_working_value = ",".join(workingtimeList) + ');'
        sql_working_insert = "INSERT INTO WK_B_WORKINGTIME_BASE VALUES(" + sql_working_value
        sql_working_delete = "DELETE FROM WK_B_WORKINGTIME_BASE WHERE YEARMONTH = {}".format(nengetu)
        print(sql_working_delete)
        print(sql_working_insert)
        cur.execute(sql_working_delete)
        cur.execute(sql_working_insert)

        #年間集計
        sum_dayCnt += dayCnt
        sum_timeCnt = sum_timeCnt + (dayCnt * 8)
        sum_holCnt += holCnt
        sum_fullDay += fullDay

    print("////////////////////////////")
    print("年間営業日数 = " + str(sum_dayCnt))
    print("年間月基準時間 = " + str(sum_timeCnt))
    print("年間休日日数 = " + str(sum_holCnt))
    print("年間総日数 = " + str(sum_fullDay))
    #端数処理は銀行丸め処理にて
    fsum_timeCnt = float(sum_timeCnt / 12)
    fsum_dayCnt = float(sum_dayCnt / 12)
    _start = start.split('-')
    periodList = [str(period),  _start[0] + _start[1],str(sum_fullDay), str(sum_dayCnt),str(sum_holCnt), str(round(fsum_dayCnt,2)), str(round(fsum_timeCnt,2))]
    sql_values = ",".join(periodList) + ');'
    sql_insert = "INSERT INTO  WK_M_PERIOD VALUES(" + sql_values
    sql_delete: str = 'DELETE FROM WK_M_PERIOD WHERE BASIC_WORKING_HOURS_PERIOD = {}'.format(str(period))
    print(sql_delete)
    cur.execute(sql_delete)
    print(sql_insert)
    cur.execute(sql_insert)
    conn.commit()

except:
    conn.rollback()
    import traceback
    traceback.print_exc()
finally:
    cur.close
    conn.close
