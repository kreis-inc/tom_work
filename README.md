## tom_work　クライス勤怠システム

[！[]（http://img.youtube.com/vi/RrgJArxZTw4/0.jpg）]（http://www.youtube.com/watch?v=RrgJArxZTw4 "web bj"）

### ＜各種プログラム概要＞
   #### 1. 勤務表一括取込処理　delin_worklist.py
【概要】
   * 勤務表ExcelをDBへ登録する補助バッチプログラム　フェーズ２のSlackFormリリースまでの暫定プログラムとなる。

#### 2.年間就労時間算出　basetime.py   
【概要】
   * 年間営業日数、月別での営業日数、月別での就労時間を算出するプログラム。
      基本期初に１年分作成を行う。休日マスターの変更があった場合は際実行を行います。基本毎年6月に１回実行し、7月よりの新期前に実行を行う。

#### 3.月次勤怠締め処理　working_sum.py
【概要】
   * 勤務表（WK_B_WORKIN_LIST) から指定月の勤務時間集計を算出するプログラム。
   　結果は、WK_B_WOKING_SUMテーブルへ格納されます。

#### 4.レポート作成処理　create_report.py

###＜プログラム実行環境＞
* 言語　→　python3.8環境
* DB　→　Server version: 5.6.10 MySQL Community Server (GPL)
