#!/bin/bash
clear
 ▄█     █▄     ▄████████ ▀█████████▄  ▀█████████▄       ▄█↲
███     ███   ███    ███   ███    ███   ███    ███     ███↲
███     ███   ███    █▀    ███    ███   ███    ███     ███↲
███     ███  ▄███▄▄▄      ▄███▄▄▄██▀   ▄███▄▄▄██▀      ███↲
███     ███ ▀▀███▀▀▀     ▀▀███▀▀▀██▄  ▀▀███▀▀▀██▄      ███↲
███     ███   ███    █▄    ███    ██▄   ███    ██▄     ███↲
███ ▄█▄ ███   ███    ███   ███    ███   ███    ███     ███↲
 ▀███▀███▀    ██████████ ▄█████████▀  ▄█████████▀  █▄ ▄███↲
                                                   ▀▀▀▀▀▀↲
    ███      ▄██████▄  ▄██   ▄       ███      ▄██████▄   ▄██████▄   ▄█↲
▀█████████▄ ███    ███ ███   ██▄ ▀█████████▄ ███    ███ ███    ███ ███↲
   ▀███▀▀██ ███    ███ ███▄▄▄███    ▀███▀▀██ ███    ███ ███    ███ ███↲
    ███   ▀ ███    ███ ▀▀▀▀▀▀███     ███   ▀ ███    ███ ███    ███ ███↲
    ███     ███    ███ ▄██   ███     ███     ███    ███ ███    ███ ███↲
    ███     ███    ███ ███   ███     ███     ███    ███ ███    ███ ███↲
    ███     ███    ███ ███   ███     ███     ███    ███ ███    ███ ███▌    ▄↲
   ▄████▀    ▀██████▀   ▀█████▀     ▄████▀    ▀██████▀   ▀██████▀  █████▄▄██↲
                                                                   ▀↲
           ╔╦╗╔═╗╔╦╗    ╦ ╦╔═╗╦═╗╦╔═╦╔╗╔╔═╗  ╔═╗╔═╗╔═╗↲
─────────   ║ ║ ║║║║    ║║║║ ║╠╦╝╠╩╗║║║║║ ╦  ╠═╣╠═╝╠═╝  ─────────↲
            ╩ ╚═╝╩ ╩    ╚╩╝╚═╝╩╚═╩ ╩╩╝╚╝╚═╝  ╩ ╩╩  ╩↲

echo "<<勤怠システム>> ";
echo "------------------------------------ ";
echo " lo, ログイン ";
echo " 1,  勤務表取込 ";
echo " 2,  締処理状況確認（実行月確認） ";
echo " 3,  締処理削除（１回分前月に戻す） ";
echo " 4,  締処理実行 ＆ Excel生成";
echo " 5,  Excel生成 ";
echo " 6,  フレックス時間確認 ";
echo " 7,  有給台帳　取得 ";
echo " 8,  勤務表DB(Workinglist) 取得";
echo " 9,  集計勤務表DB(WorkingSum)取得 ";
echo " 10,  DBバックアップ ";
echo " 11,  Manuへ";
echo " 12,  処理終了";
echo "------------------------------------ ";
echo -n " 実行No : "
read SAVE_METHOD

case "$SAVE_METHOD" in
 lo)
    eval $line;;
  0 )
    sql="${line} -N -e \"show tables like 'WK%'\" |pbcopy"
esac
