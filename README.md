交易程式1.1.0版本更新速報
1. 修正了不會計算相似度計算連動度的問題。
2. 現在程式會先計算連動度，再依照結果去選股。
3. 程式現在會在「更新K線數據」時一併計算族群連動度、並進行分析的預加載。
4. 管理族群功能變得更好看。
5. 修正了一些進場的錯誤判斷。
6. 提高了極大化利潤功能的速度，但輸出有點醜。
7. 目前還不能在使用者介面自定義分析的範圍，只能ctrl + F全域搜尋「wait_minutes_range」和「hold_minutes_range」來修改。
8. 開始交易功能還不能用。
9. 請注意要安裝以下環境：
    (1)目前只支援Python 3.10
    (2)如果執行後會馬上報錯，請輸入以下指令：
   pip install fugle-marketdata pandas pyyaml colorama numpy python-dateutil tabulate openpyxl
