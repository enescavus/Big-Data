###################################################################################################################
#  author : ENES ÇAVUŞ
#  subject : Python file for managing the other Python files at once 
###################################################################################################################

import pandas as pd 
from pyhive import hive
import time
import os 

# hive server 2 connectin via a new port
conn = hive.Connection(host="localhost", port=10000, username="ubuntu3")

#hiveserver2 &

cursor = conn.cursor()


# this is a command line manager to watch the process of real time data PULLING - STREAMING - MAP REDUCE and UPDATING ON DASH PLOTLY
while True:
    print("Tablolar Güncellenmek Üzere Temizleniyor")
    truncate1 = 'TRUNCATE TABLE trends' # clean the trends table
    truncate2 = 'TRUNCATE TABLE trendslocal' # clean the trends table 
    print("Tablolar Temizlendi - Veriler Güncellenecek - Son 5 Saniye")
    time.sleep(5)
    cursor.execute(truncate1)
    cursor.execute(truncate2)
    os.system('python3 TrendProducer.py')
    print("Gerçek Zamanlı Veriler Çekiliyor - Lütfen Bekleyin - Son 5 Saniye")
    for i in range(5):
        time.sleep(1)
    print("Veriler 60 Saniye İçinde Yeniden Güncellenecek")
    for i in range(60):
        print("Son ", int(60 - i) )
        time.sleep(1)
    os.system('python3 DashUzerindeUpdate.py')
    print("Process starts Again!!!----------------------------------")
    

# END - ENES ÇAVUŞ - Btirme Projesi - SAU - Bahar 2021
