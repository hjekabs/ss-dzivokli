import requests
import hashlib
import json
import sqlite3
import time
import schedule
from bs4 import BeautifulSoup


def telegram_bot_sendtext(bot_message):
    
    bot_token = '1351614418:AAFoRjC0mMZLItL1zSAUAjHm-UPhfX09bTA'
    bot_chatID = '-411016673'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)
    return response.json()




URLS = [
    "https://www.ss.lv/lv/real-estate/flats/riga/all/hand_over/filter/",
    "https://www.ss.com/lv/real-estate/flats/riga/all/hand_over/filter/page2.html",
    "https://www.ss.com/lv/real-estate/flats/riga/all/hand_over/filter/page3.html",
    "https://www.ss.com/lv/real-estate/flats/riga/all/hand_over/filter/page4.html",
]



def main_process(base_url):
    res = requests.get(base_url)
    ss_html = res.text
    soup = BeautifulSoup(ss_html, 'html.parser')
    table = soup.find_all("table")
    dzivoklu_table = table[1].find_all("table")
    final_table = dzivoklu_table[2]
    apartments = final_table.find_all("tr")
    potential_apartments = []
    all_apartments = []

    for ap in apartments:


        td = ap.find_all("td") 

        all_apartments.append(td)

    clean_apartments = all_apartments[2:]

    for td in clean_apartments:

        rooms = td[4].text
        price = td[8].text.replace(" €/mēn.", "")
        price = price.replace(" €/dienā", "")

        if td[4].text == "2" and int(price) >= 300 and int(price) <= 500:
            potential_apartments.append(td)


    # This is the final clean list
    clean_list_aparments = []

    for apartment in potential_apartments:

        obj = {}

        obj["cena"] = apartment[8].text
        obj["adrese"] = apartment[3].text

        for td in apartment:
        
            anchor = td.find("a")
            if anchor:
                obj["url"] = anchor["href"]
                
        clean_list_aparments.append(obj)

    # save to db

    conn = sqlite3.connect("test.db")
    c = conn.cursor()

    sqlite_select_query = """SELECT * from apartments"""
    c.execute(sqlite_select_query)
    records = c.fetchall()

    db_records = []
    for record in records:
        r = json.loads(record[0])
        db_records.append(r)

    for a in clean_list_aparments:
        print(a)
        if a not in db_records:
            print("not in db so should save")
            c.execute("insert into apartments values (?)",
                [json.dumps(a)])
            conn.commit()
            message = "Jauns dzivoklis: " + a["cena"] + " " + a["adrese"] + " www.ss.com/" + a["url"]
            telegram_bot_sendtext(message)
        else:
            print("In db so we are skipping")


    conn.close()

def job():
    for url in URLS:
        try:
            main_process(url)
        except ValueError as ex:
            print(ex)

schedule.every(30).minutes.do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)


