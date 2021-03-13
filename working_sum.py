#  _____          _____           _           _____ ___  __  __
# |_   _|__  _   |_   _|__   ___ | |         |_   _/ _ \|  \/  |
#   | |/ _ \| | | || |/ _ \ / _ \| |  _____    | || | | | |\/| |
#   | | (_) | |_| || | (_) | (_) | | |_____|   | || |_| | |  | |
#   |_|\___/ \__, ||_|\___/ \___/|_|           |_| \___/|_|  |_|
#           | ___ /
#
# <プログラム名＞　
#  working_sum.py ：　勤務一覧集計バッチ
# <概要＞
# 他プログラム依存しないように集計処理に関しては１ファイルで完結するようにする事。
#
#   1,集計を行う年月は、締処理管理テーブルの年月を取得し実施。
#       締処理管理テーブルの年月MAX　＋　１ヶ月の年月にて実施。
#       締処理管理テーブルは正常終了後に登録します。正常終了後再度締め処理を実行する場合は手動にて
#       レコードを削除しRERUNが必要。
#   2,集計を行う社員は社員マスタに登録されている社員分の集計実施を行う。（削除フラグNULLの社員)
#   3,実施START、END、LOGに関してはすべてSlackへ送信。
#     総務一覧チャンネルで締計算を実施した旨を社員に通達。
#
# ＜ロジック概要 Loopのネスト処理概要＞
#  社員マスターで１User毎Loop:
#       user、年月で勤務表を取得し1day毎Loop;
#             1.1日の勤務時間を算出。
#             2.日曜日先頭とした週Noの付与。
#             3,振替ロジック用に休日(土日祝）で８時間勤務のした日の印付け。
#             4,振替、有給、慶弔休暇、その他休暇処理をしやすい様にに各種List作成を行う。
#     　振替ロジックLoop：
#     　有給取得ロジックloop:
#     　その他休暇、慶弔休暇ロジックloop:
#       1User毎の集計処理しDB登録。
#  全ユーザの集計処理終了後、Slackへ集計バッチ終了報告。
#  END
#
# created by BJProject(ichio.kondo)

import MySQLdb
import datetime
import yaml
import math
from dateutil.relativedelta import relativedelta

# 区分値のconfig読み込み
with open('kbn.yml') as kfile:
    kyml = yaml.load(kfile)


# 0 実行年月取得
def getExcecDate( _cur ):
    date_sql = "select max(YEARMONTH) from WK_B_PROCESS"
    cur.execute(date_sql)
    execDateRecordes = _cur.fetchall()
    for record in execDateRecordes:
        return record


# 正常終了時にWK_B_PROCESSに締処理完了を登録。
def setExecDate( _cur, _nengetu ):
    _sql = "INSERT INTO WK_B_PROCESS VALUES('{0}',now())".format(_nengetu)
    _cur.execute(_sql)


# 1 社員マスタより社員IDと社員名のList取得
def getEmployee( _cur ):
    emp_sql = 'SELECT * FROM WK_M_EMPLOYEE WHERE 削除フラグ IS NULL or 削除フラグ ="0"'
    _cur.execute(emp_sql)
    emp_recordes = _cur.fetchall()
    emp = []
    for record in emp_recordes:
        emp.append([record[0], record[1], record[2]])
    return emp


# 2 祝日マスタ取得
def getHolidayList( cur ):
    # 祝日マスター取得
    holiday_sql = 'SELECT * FROM WK_M_HOLIDAY'
    cur.execute(holiday_sql)
    records = cur.fetchall()
    holiday = []
    for record in records:
        holiday.append(record[0])
    return holiday


# 3 社員の有給残数取得するメソッド
def getPaid( cur, nengetu, employee_no ):
    paid_sql = "select sum(NUMBER_OF_PAID) FROM WK_B_PAID where EMPLOYEE_NO = {0} AND DATE_FORMAT(YEARMONTHDAY, '%Y%m')  <= {1}".format(
        employee_no, nengetu)
    cur.execute(paid_sql)
    records = cur.fetchall()
    paidList = []
    for record in records:
        if record is None:
            paidList.append(0)
        else:
            paidList.append(record[0])
    # 有給残数取得
    paidZan = 0
    if paidList[0] is not None:
        paidZan = float(paidList[0])
    return paidZan


# 3.1 当月有給付与分取得
def getCurentPaid( cur, employee_no, nengetu ):
    paid_sql = "SELECT * FROM WK_B_PAID WHERE PAID_KBN = '有給付与' AND EMPLOYEE_NO = {0}  AND DATE_FORMAT(YEARMONTHDAY, '%Y%m')  = {1}".format(
        employee_no, nengetu)
    cur.execute(paid_sql)
    recordes = cur.fetchall()
    cnt = 0
    for record in recordes:
        if record is not None:
            cnt += record[3]
    return cnt


# 3.2 有給削除
def delPaid( cur, employee_no, nengetu ):
    paid_del_sql = "delete from WK_B_PAID  where  PAID_KBN <> '有給付与' AND PAID_KBN <> '有給残登録' AND EMPLOYEE_NO = {0}  AND DATE_FORMAT(YEARMONTHDAY, '%Y%m')  = {1}".format(
        employee_no, nengetu)
    cur.execute(paid_del_sql)


# 4 有給消化,休暇取得Insert分発行メソッド
def setPaid( _cur, employee_no, yearmonthday ):
    paid_insert_sql = "INSERT INTO WK_B_PAID VALUES({0},'{1}','{2}',{3},'{4}')".format(employee_no, yearmonthday,
                                                                                       "有給", "-1", "")
    _cur.execute(paid_insert_sql)


# 　半休取得Insert
def setPaidHalf( cur, employee_no, yearmonthday ):
    paid_insert_sql = "INSERT INTO WK_B_PAID VALUES({0},'{1}','{2}',{3},'{4}')".format(employee_no, yearmonthday,
                                                                                       "半休", "-0.5", "")
    cur.execute(paid_insert_sql)


#  慶弔休暇Insert
def setPaidKeicho( cur, employee_no, yearmonthday, remarks ):
    paid_insert_sql = "INSERT INTO WK_B_PAID VALUES({0},'{1}','{2}',{3},'{4}')".format(employee_no, yearmonthday,
                                                                                       "慶弔休暇", "0", remarks)
    cur.execute(paid_insert_sql)


# その他休暇Insert
def setPaidEtc( cur, employee_no, yearmonthday, remarks ):
    paid_insert_sql = "INSERT INTO WK_B_PAID VALUES({0},'{1}','{2}',{3},'{4}')".format(employee_no, yearmonthday,
                                                                                       "その他休暇", "0", remarks)
    cur.execute(paid_insert_sql)


# 振替休暇Insert
def setPaidDaikyu( cur, employee_no, yearmonthday, remarks ):
    paid_insert_sql = "INSERT INTO WK_B_PAID VALUES({0},'{1}','{2}',{3},'{4}')".format(employee_no, yearmonthday,
                                                                                       "振替休暇", "0", remarks)
    cur.execute(paid_insert_sql)


# 有給消失時用Insert
def setPaidDisappear( cur, employee_no, yearmonth, number ):
    yearmonthday = str(yearmonth) + "01"  # 1日として喪失データ作成を行う。
    paid_insert_sql = "INSERT INTO WK_B_PAID VALUES({0},'{1}','{2}',{3},'{4}')".format(employee_no, yearmonthday,
                                                                                       "有給喪失", number, "")
    cur.execute(paid_insert_sql)


# 所定労働日数、所定労働時間を取得
def getBasetime( cur, yearmonth ):
    select_sql = "select * from WK_B_WORKINGTIME_BASE where YEARMONTH = {0}".format(yearmonth)
    cur.execute(select_sql)
    records = cur.fetchall()
    baseList = []
    for record in records:
        baseList.append([record[4], record[2]])
    return baseList


# 7 勤務表取得
def getWorkingList( cur, nengetu, employee_no ):
    workinglist_sql = "select * from WK_B_WORKIN_LIST where DATE_FORMAT(YEARMONTHDAY, '%Y%m') = {0}  and EMPLOYEE_NO = {1}".format(
        nengetu, employee_no)
    cur.execute(workinglist_sql)
    records = cur.fetchall()
    wList = []
    for record in records:
        wList.append(
            [record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]])
    return wList


# 祝日チェック
def isHoliday( holidayList, day ):
    for record in holidayList:
        if record[0].strftime('%Y%m%d') == day.strftime('%Y%m%d'):
            return True
    return False


errorCnt = 0
loglist = []
print( " ______  _____                ____       _____     ")
print( "/\__  _\/\  __`\  /'\_/`\    /\  _`\    /\___ \    ")
print( "\/_/\ \/\ \ \/\ \/\      \   \ \ \L\ \  \/__/\ \   ")
print( "   \ \ \ \ \ \ \ \ \ \__\ \   \ \  _ <'    _\ \ \  ")
print( "    \ \ \ \ \ \_\ \ \ \_/\ \   \ \ \L\ \__/\ \_\ \ ")
print( "     \ \_\ \ \_____\ \_\\ \_\   \ \____/\_\ \____/ ")
print( "      \/_/  \/_____/\/_/ \/_/    \/___/\/_/\/___/  ")
print( "                                                   ")
print( " Kreis 勤怠集計プログラムSTART!! ")
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

    # 実行年月取得 年月をDateTimeに変換し１ヶ月加算し再度文字列に変換し先頭６文字を取得。
    execDate = getExcecDate(cur)
    _execDate = str(execDate[0])
    Date = datetime.date(int(_execDate[0:4]), int(_execDate[4:6]), 1)
    tdate = Date + relativedelta(months=1)
    nengetu = str(tdate.year) + str(tdate.month).zfill(2)  # 年月↲

    # 社員マスター取得
    empList = getEmployee(cur)
    baselist = getBasetime(cur, nengetu)
    basictime = float(baselist[0][0])
    basicday = float(baselist[0][1])

    # 祝日マスター取得
    holidayList = getHolidayList(cur)
    loglist.append("勤務表集計処理START" + " : " + nengetu + " の処理")
    # 社員マスター分loop
    print("■" + nengetu + "　を勤務集計処理を開始します。")

    # １User分の集計レコード作成しコミットとする。エラー発生した場合は次User処理を行う。
    for empNo in empList:
        try:
            # Rerunを考えて実施月の有給を削除 deletge →　insert
            delPaid(cur, empNo[0], nengetu)
            # 当月の有給付与がある確認
            curentPaid = getCurentPaid(cur, empNo[0], nengetu)
            # 有給残数を確認　尚max有給数を超えている場合は消去レコードを作成。
            startzan = getPaid(cur, nengetu, empNo[0])
            # 　有給MAX分より有給消失を登録
            if 40 < startzan:
                delPCnt = 40 - startzan
                # 有給消去レコード登録
                setPaidDisappear(cur, empNo[0], nengetu, delPCnt)
                startzan = getPaid(cur, nengetu, empNo[0])
            # 年月と社員IDで勤務表取得　WK_B_WORKING_LIST
            # 勤務一覧取得 しloop
            kinmuList = getWorkingList(cur, nengetu, empNo[0])
            templist = []
            dayDic = {}  # 日付：List
            paidlist = []  # 休暇（その他以外）　週番号：日付
            etclist = []
            keichoList = []  # 慶弔休暇リスト　
            kekinCnt = 0
            remarks = ""
            paid_cnt = 0  # 全種類の休暇数
            for kinmu in kinmuList:
                # 0 YEARMONTHDAY   年月日
                # 1 EMPLOYEE_NO    社員No :
                # 2 YOBI           曜日  :
                # 3 START_TIME     出社時刻:
                # 4 END_TIME       退社時刻:
                # 5 BREAK_TIME     休憩時間 :
                # 6 休日区分       休日区:
                # 7 休暇区分       休暇区:
                # 8 その他休暇種類   備考：
                day = kinmu[0]
                yobi = kinmu[2]
                start = kinmu[3]
                end = kinmu[4]
                breaktime = kinmu[5]
                paid = kinmu[7]
                # 　休暇区分を入力した日が休日だった場合は、入力ミス。
                if paid == "休暇" or paid == "半休" or paid == "慶弔休暇" or paid == "その他休暇":
                    if yobi == "土" or yobi == "日" or yobi == "祝":
                        paid = ""
                    elif paid == "半休":
                        paid_cnt += 0.5
                    else:
                        paid_cnt += 1

                remarks = kinmu[8]
                weekno = day.strftime("%U")
                # 10進変換　開始時間
                sf, si = math.modf(float(start))
                _start = si + float('{:1.02f}'.format(sf * 5 / 3))
                # 10進変換　終了時間
                ef, ei = math.modf(float(end))
                _end = ei + float('{:0.02f}'.format(ef * 5 / 3))
                # 勤務時間
                worktime = _end - _start - float(breaktime)
                # 深夜残業
                orvertime = 0
                if _end >= 22 and _start >= 5:
                    orvertime = _end - 22
                # 休日フラグ 2→振替出社候補 1→休日出社８時間未満
                if yobi == '土' or yobi == '日' or yobi == '祝':
                    if worktime >= 8:
                        holidayflg = 2
                    elif worktime >= 0.25:
                        holidayflg = 1
                    else:
                        holidayflg = 0
                else:
                    holidayflg = 0
                # テンポラリー配列 [ 日付、曜日、週No,勤務時間、深夜残業時間、休暇種類、休日出社フラグ]
                # 配列を扱う為の配列番号用定数定義
                _day_ = 0  # 日付
                _yobi_ = 1  # 曜日
                _weekno_ = 2  # 週No
                _worktime_ = 3  # 勤務時間
                _orvertime_ = 4  # 深夜残業時間
                _paid_ = 5  # 休暇種類　休暇、半休、その他
                _holidayflg_ = 6  # 休日出社フラグ  2→振替出社候補 1→休日出社８時間未満
                _remarks_ = 7
                # 配列に格納
                daylist = [day, yobi, weekno, worktime, orvertime, paid, holidayflg, remarks]
                templist.append(daylist)  # 集計時に利用
                # 日付をKEYに配列格納。取り出しやすい用に　日付：配列
                dayDic[day] = daylist
                # 有給半休処理用に日付を別リストへ 有給半休=paidList,慶弔休暇=keichoList,その他休暇=etclist

                if paid == "休暇" or paid == "半休":
                    paidlist.append(day)
                elif paid == "その他休暇":
                    etclist.append(day)
                elif paid == "慶弔休暇":
                    keichoList.append(day)

            # 振替交換処理　→　有給取得処理、→　慶弔休暇処理、→　その他休暇処理の順番で処理
            # すべて、WK_B_PAIDテーブルへ登録を行い管理する。

            # 振替付与ロジック
            #    ・週番号内で振替となり得る休暇を検索し、交換日に’振替休暇’の印をつける。
            #      又、交換日が見つかった場合は、休暇した日は ’振替休暇’の印をつけておき、有給にならないようにする。
            #      交換日は、休暇出社フラグ ’２’がついている8h以上勤務のものがターゲットとなる。
            paidRemarksDic = {}
            daikyuCnt = 0  # 振替カウント用
            for day in paidlist:
                chengeDay = ""
                wn = (dayDic[day])[_weekno_]
                if (dayDic[day])[_paid_] == "半休":
                    continue
                for daylist in templist: # 週番号が同じで且、休暇出社フラグが２のもがあった場合、以下処理
                    if daylist[_weekno_] == wn and daylist[_holidayflg_] == 2:
                        # 1,休暇種類を休暇→振替出社とし印
                        daylist[_paid_] = "振替出社"
                        # 2,休日出社フラグを２から３に変更する
                        daylist[_holidayflg_] = 3
                        shushaDay = daylist[_day_].strftime('%Y-%m-%d')
                        (dayDic[day])[_paid_] = "振替休暇"
                        (dayDic[day])[_worktime_] = 0  # 振替休暇なので勤務時間は０とする。
                        kyuka = day.strftime('%Y-%m-%d')
                        remarksVal = shushaDay + " 出社の振替休暇"
                        # WK_B_PAIDへ登録
                        setPaidDaikyu(cur, empNo[0], kyuka, remarksVal)
                        paidRemarksDic[day] = remarksVal
                        daikyuCnt += 1
                        break

            # 有給,半休ロジック
            # 振替休暇、慶弔休暇、その他休暇意外の休暇処理。有給残を確認しながら、有給か欠勤かの判断をする。
            # 有給の場合（残数がある場合）はデータベースへの有給登録とworktimeに8h又は4hを登録。

            paidZan = getPaid(cur, nengetu, empNo[0])  # 有給数取得
            paidCnt = 0
            for day in paidlist:
                stringday = day.strftime('%Y-%m-%d')
                if (dayDic[day])[_paid_] == "休暇":
                    if paidZan >= 1:
                        # 有給取得
                        paidZan = paidZan - 1
                        (dayDic[day])[_paid_] = "有給"
                        # 有給テーブルへ登録
                        setPaid(cur, empNo[0], stringday)
                        # 勤務時間に８時間を付与
                        (dayDic[day])[_worktime_] = 8
                        paidCnt += 1

                    elif paidZan >= 0.5:  # 有給残が0.5しかない場合は休暇→半休にして４時間のみ付与。
                        # 半休取得
                        paidZan = paidZan - 0.5
                        (dayDic[day])[_paid_] = "半休"
                        setPaidHalf(cur, empNo[0], stringday)
                        (dayDic[day])[_worktime_] += 4
                        paidCnt += 0.5
                    else:
                        # 欠勤
                        (dayDic[day])[_paid_] = "欠勤"
                        (dayDic[day])[_worktime_] = 0
                        kekinCnt += 1
                elif (dayDic[day])[_paid_] == "半休":
                    if paidZan >= 0.5:
                        # 半休取得
                        paidZan = paidZan - 0.5
                        (dayDic[day])[_paid_] = "半休"
                        setPaidHalf(cur, empNo[0], stringday)
                        (dayDic[day])[_worktime_] += 4
                        paidCnt += 0.5
                    else:
                        # 欠勤
                        (dayDic[day])[_paid_] = ""

            # 慶弔休暇ロジック　DBへ登録しworktimeに８時間付与
            for day in keichoList:
                setPaidKeicho(cur, empNo[0], day.strftime("%Y-%m-%d"), dayDic[day][_remarks_])
                (dayDic[day])[_worktime_] = 8

            # その他休暇ロジック　DBへ登録　worktimeは0h
            for day in etclist:
                setPaidEtc(cur, empNo[0], day.strftime("%Y-%m-%d"), dayDic[day][_remarks_])
                (dayDic[day])[_worktime_] = 0

            # すべての休暇ロジックが終了した為、必要な項目の集計を行う。
            # 有給付与ロジック後の有給残を取得する。月末時点 有給残数取得
            paidZanNext = getPaid(cur, nengetu, empNo[0])

            # -----------------------------------------------------------------
            # 　20200725 →　仕様追加分　休暇日数、法定休日出社日数、法定外休日出社日数のカウントをする。
            # 集計ロジック
            sum_working_time = 0  # 総労働時間
            sum_kekkin_time = 0  # 欠勤控除対象時間
            # 固定残業時間
            sum_choka_time = 0  # 超過残業時間
            sum_sinya_time = 0  # 深夜残業時間
            sum_kyujituzangyo_time = 0  # 休日残業時間
            sum_kyujituzangyo_cnt = 0  # 休日残業としてカウントする日数用
            sum_sunday_time = 0  # 法定休日の出社時間合計
            sum_sunday_work_cnt = 0  # 法定休日出社日数
            sum_holiday_work_cnt = 0  # 法定外出社日数

            for tl in templist:
                sum_working_time += tl[_worktime_]
                sum_sinya_time += tl[_orvertime_]
                # 休日残業時間算出　日曜日で振替出社以外の時間合計
                if tl[_yobi_] == "日":
                    sum_sunday_time += tl[_worktime_]
                if tl[_yobi_] == "日" and tl[_holidayflg_] != 3:
                    sum_kyujituzangyo_time += tl[_worktime_]
                    if tl[_worktime_] > 0:
                        sum_kyujituzangyo_cnt += 1

                # 休日出社日数カウント　内（法定外休日、法定休日に分ける）
                if tl[_yobi_] == "日" and tl[_worktime_] > 0:
                    sum_sunday_work_cnt += 1
                    print("#################################### 休日出社カウントされた＝ " + str(empNo[1]) + str(tl[_day_]))
                elif tl[_yobi_] == "土" and tl[_worktime_] > 0:
                    sum_holiday_work_cnt += 1
                    print("#################################### 土曜日出社カウントされた＝ " + str(tl[_day_]))
                elif tl[_yobi_] == "祝" and tl[_worktime_] > 0:
                    sum_holiday_work_cnt += 1
                    print("#################################### 祝日出社カウントされた＝ " + str(tl[_day_]))

            # 超過残業時間算出　所定時間ー勤務時間ー休日残業時間
            _sum_working_time = sum_working_time - sum_kyujituzangyo_time
            if float(basictime) < sum_working_time and _sum_working_time - float(basictime) - 30 > 0:
                sum_choka_time = sum_working_time - float(basictime) - 30
            elif float(basictime) - sum_working_time > 0:
                # 欠勤
                sum_kekkin_time = float(basictime) - sum_working_time

            ### 登録DBのWK_B_WOKING_SUM のカラム順　valuesを下記順番通りにlistへappend
            # 社員,年月,社員名
            # 所属事業所,所定就労日,所定就労時間
            # 総労働時間,欠勤控除対象時間,固定時間外勤務時間
            # 超過時間外勤務時間,深夜勤務時間
            # 休日勤務時間,休日手当支給日数
            # 休日手当支給時間,
            # 有給付与数,有給日数,
            # 有給残数,慶弔休暇数
            # 特別休暇日数,振替休暇日数
            # 休暇日,法定外休日出社日数
            # 法定休日出社日数
            # 11g 処理実施時間

            values = ['"' + str(empNo[0]) + '"', '"' + str(nengetu) + '"', '"' + str(empNo[1]) + '"',
                      '"' + str(empNo[2]) + '"', '"' + str(basicday) + '"', '"' + str(basictime) + '"',
                      '"' + str(sum_working_time) + '"', '"' + str(sum_kekkin_time) + '"', '"' + str(30) + '"',
                      '"' + str(sum_choka_time) + '"', '"' + str(sum_sinya_time) + '"',
                      '"' + str(sum_sunday_time) + '"', '"' + str(sum_kyujituzangyo_cnt) + '"', '"' + str(sum_kyujituzangyo_time) + '"',
                      '"' + str(curentPaid) + '"', '"' + str(paidCnt) + '"',
                      '"' + str(paidZanNext) + '"', '"' + str(len(keichoList)) + '"',
                      '"' + str(len(etclist)) + '"', '"' + str(daikyuCnt) + '"',
                      '"' + str(paid_cnt) + '"', '"' + str(sum_holiday_work_cnt) + '"',
                      '"' + str(sum_sunday_work_cnt) + '"',
                      "now()"]

            valustr = ",".join(values)
            valurecode = "(" + valustr + ")"

            # delete - insert実施 リランを考慮。尚、有給管理テーブルは有給付与バッチ前にDeleteし新規作成している。
            delsql = "delete from WK_B_WOKING_SUM WHERE EMPLOYEE_NO= '{0}' and YEARMONTH = {1} ".format(empNo[0],
                                                                                                        nengetu)
            cur.execute(delsql)
            sql = "INSERT INTO WK_B_WOKING_SUM VALUES{0};".format(valurecode)
            cur.execute(sql)

            # confirmテーブルへdelete insert
            confdelsql = "DELETE FROM WK_B_WORKIN_LIST_CONFIRM WHERE EMPLOYEE_NO = '{0}' AND DATE_FORMAT(YEARMONTHDAY, '%Y%m') = '{1}'".format(
                empNo[0], nengetu)
            cur.execute(confdelsql)

            for tl in templist:
                _paidremarks = paidRemarksDic.get(tl[0], "")
                confvalues = ['"' + str(empNo[0]) + '"',
                              '"' + str(tl[0]) + '"',
                              '"' + tl[_yobi_] + '"',
                              '"' + str(tl[_worktime_]) + '"',
                              '"' + tl[_paid_] + '"',
                              '"' + _paidremarks + '"']
                confvalustr = ",".join(confvalues)
                confvalurecode = "(" + confvalustr + ")"
                confInsertsql = "INSERT INTO WK_B_WORKIN_LIST_CONFIRM VALUES{0};".format(confvalurecode)
                cur.execute(confInsertsql)
            conn.commit()

            # 以下実行ログ出力ロジック。
            loglist.append("<<集計情報>>")
            loglist.append("{0} : {1} 処理日数{2}日分".format(empNo[0], empNo[1], str(len(kinmuList))))
            loglist.append("{0}月ー所定時間 : {1}".format(nengetu, str(basictime)))
            loglist.append("有給付与 : {0}".format(curentPaid))  # curentPaid
            loglist.append("総労働時間 :  {0}h".format(str(sum_working_time)))
            loglist.append("残業時間   :  {0}h".format(str(sum_choka_time)))
            loglist.append("休日出社時間： {0}h".format(str(sum_kyujituzangyo_time)))
            loglist.append("深夜残業 :    {0}h".format(str(sum_sinya_time)))
            loglist.append("欠勤時間 :    {0}h".format(str(sum_kekkin_time)))
            if len(kinmuList) > 0:
                loglist.append("　-- <<休暇内容>>")
                loglist.append("　-- 処理前有給残数 : {0}".format(str(startzan)))
                loglist.append("　-- 有給休暇数 : {0}".format(str(paidCnt)))
                loglist.append("　-- 振替休暇数 : {0}".format(str(daikyuCnt)))
                loglist.append("　-- その他休暇数 : {0}".format(str(len(etclist))))
                loglist.append("　-- 欠勤 　　　　: {0}".format(str(kekinCnt)))
                loglist.append("　-- 処理後有給残数 : {0}".format(str(paidZanNext)))
                loglist.append("")
            loglist.append("----------------------------------------")
        except:
            # Ngなので次の社員へ
            print(empNo[1] + " = エラー")
            loglist.append(empNo[1] + " = エラー")
            import traceback

            traceback.print_exc()
            errorCnt += 1
            continue

    print('\n'.join(loglist))
    if errorCnt == 0:
        setExecDate(cur, nengetu)
        conn.commit()
        print("{0}月分の勤怠締め処理、正常終了　{1}件のデータを登録しました。　→　締め処理管理テーブル（WK_B_PROCESS)へ{2}月締処理完了登録をしました。".format(nengetu, str(
            len(empList)), nengetu))
    else:
        print("errorが{0}件発生しています。エラー箇所確認して下さい。".format(str(errorCnt)))
except:
    conn.rollback()
    print(" 異常終了しました。維持管理まで連絡")
    loglist.append("異常終了　維持管理まで連絡")
    import traceback

    traceback.print_exc()
finally:
    cur.close
    conn.close
