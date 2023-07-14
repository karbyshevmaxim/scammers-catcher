import psycopg2
import pandas as pd
import os

def correctDate(dt1, dt2):
    dt1 = dt1.split('-')
    dt2 = dt2.split('-')
    if int(dt1[0]) > int(dt2[0]):
        return False
    if int(dt1[0]) == int(dt2[0]):
        if int(dt1[1]) > int(dt2[1]):
            return False
        if int(dt1[1]) == int(dt2[1]):
            if int(dt1[2]) > int(dt2[2]):
                 return False
            return True
        return True
    return True

# Создание подключения к PostgreSQL Edu
connEdu = psycopg2.connect(database = "edu",
                        host =     "de-edu-db.chronosavant.ru",
                        user =     "de11an",
                        password = "peregrintook",
                        port =     "5432")

# Отключение автокоммита
connEdu.autocommit = False

# Создание курсора
cursorEdu = connEdu.cursor()

# Создание подключения к PostgreSQL Bank
connBank = psycopg2.connect(database = "bank",
                        host =     "de-edu-db.chronosavant.ru",
                        user =     "bank_etl",
                        password = "bank_etl_password",
                        port =     "5432")

# Отключение автокоммита
connBank.autocommit = False

# Создание курсора
cursorBank = connBank.cursor()

print('connected')

cursorEdu.execute( "truncate table de11an.kmdd_stg_transactions;" )
connEdu.commit()
cursorEdu.execute( "truncate table de11an.kmdd_stg_blacklist;" )
connEdu.commit()
cursorEdu.execute( "truncate table de11an.kmdd_stg_accounts;" )
connEdu.commit()
cursorEdu.execute( "truncate table de11an.kmdd_stg_cards;" )
connEdu.commit()
cursorEdu.execute( "truncate table de11an.kmdd_stg_clients;" )
connEdu.commit()
cursorEdu.execute( "truncate table de11an.kmdd_stg_terminals;" )
connEdu.commit()


dfTransactions = pd.read_csv('transactions_01032021.txt', sep=";", header=0)
#df2 = pd.read_csv('transactions_02032021.txt', sep=";", header=0)
#df3 = pd.read_csv('transactions_03032021.txt', sep=";", header=0)

cursorEdu.executemany( '''INSERT INTO de11an.kmdd_stg_transactions( trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal)
                            VALUES( %s, %s, %s, %s, %s, %s, %s )''', dfTransactions.values.tolist() )
connEdu.commit()
#cursorEdu.executemany( "INSERT INTO de11an.kmdd_stg_transactions( trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal) VALUES( %s, %s, %s, %s, %s, %s, %s )", df2.values.tolist() )
#cursorEdu.executemany( "INSERT INTO de11an.kmdd_stg_transactions( trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal) VALUES( %s, %s, %s, %s, %s, %s, %s )", df3.values.tolist() )
#connEdu.commit()
print('txt inserted')

dfT1 = pd.read_excel( 'terminals_01032021.xlsx', sheet_name='terminals', header=0, index_col=None )
cursorEdu.executemany( "INSERT INTO de11an.kmdd_stg_terminals( terminal_id,terminal_type, terminal_city, terminal_address) VALUES( %s, %s, %s, %s )", dfT1.values.tolist() )
connEdu.commit()

print('excel inserted')

dfP1 = pd.read_excel( 'passport_blacklist_01032021.xlsx', sheet_name='blacklist', header=0, index_col=None )
cursorEdu.executemany( "INSERT INTO de11an.kmdd_stg_blacklist( entry_dt, passport_num ) VALUES( %s, %s )", dfP1.values.tolist() )
connEdu.commit()

print('blacklists inserted')

cursorBank.execute( "SELECT * FROM info.clients" )
records = cursorBank.fetchall()
names = [ x[0] for x in cursorBank.description ]
dfBC = pd.DataFrame( records, columns = names )
cursorEdu.executemany( "INSERT INTO de11an.kmdd_stg_clients( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )", dfBC.values.tolist() )
connEdu.commit()

print('bank clients inserted')


cursorBank.execute( "SELECT * FROM info.accounts" )
records = cursorBank.fetchall()
names = [ x[0] for x in cursorBank.description ]
dfBA = pd.DataFrame( records, columns = names )
cursorEdu.executemany( "INSERT INTO de11an.kmdd_stg_accounts( account_num, valid_to, client, create_dt, update_dt) VALUES( %s, %s, %s, %s, %s )", dfBA.values.tolist() )
connEdu.commit()

print('bank accounts inserted')

cursorBank.execute( "SELECT * FROM info.cards" )
records = cursorBank.fetchall()
names = [ x[0] for x in cursorBank.description ]
dfBCa = pd.DataFrame( records, columns = names )
cursorEdu.executemany( "INSERT INTO de11an.kmdd_stg_cards( card_num, account_num, create_dt, update_dt) VALUES( %s, %s, %s, %s )", dfBCa.values.tolist() )
connEdu.commit()

print('bank cards inserted')


cursorEdu.execute( '''select 
                                stg.terminal_id, 
                                stg.terminal_type,
                                stg.terminal_city,
                                stg.terminal_address,
                                stg.update_dt, 
                                null 
                        from de11an.kmdd_stg_terminals stg
                        left join de11an.kmdd_dwh_dim_terminals tgt
                        on stg.terminal_id = tgt.terminal_id
                        where tgt.terminal_id is null''' )

records = cursorEdu.fetchall()

cursorEdu.executemany('''insert into de11an.kmdd_dwh_dim_terminals( terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt )
                                VALUES( %s, %s, %s, %s, %s, %s )''', records )
connEdu.commit()

print('stg terminals inserted')

#------------------------------------
cursorEdu.execute( '''select 
                                stg.card_num, 
                                stg.account_num,
                                stg.update_dt,
                                null
                        from de11an.kmdd_stg_cards stg
                        left join de11an.kmdd_dwh_dim_cards tgt
                        on stg.card_num = tgt.card_num
                        where tgt.card_num is null''' )

records = cursorEdu.fetchall()

cursorEdu.executemany('''insert into de11an.kmdd_dwh_dim_cards( card_num, account_num, create_dt, update_dt )
                                VALUES( %s, %s, %s, %s )''', records )
connEdu.commit()

print('stg cards inserted')


#------------------------------------
cursorEdu.execute( '''select 
                                stg.account_num, 
                                stg.valid_to,
                                stg.client,
                                stg.update_dt,
                                null
                        from de11an.kmdd_stg_accounts stg
                        left join de11an.kmdd_dwh_dim_accounts tgt
                        on stg.account_num = tgt.account_num
                        where tgt.account_num is null''' )

records = cursorEdu.fetchall()

cursorEdu.executemany('''insert into de11an.kmdd_dwh_dim_accounts( account_num, valid_to, client, create_dt, update_dt )
                                VALUES( %s, %s, %s, %s )''', records )
connEdu.commit()

print('stg accounts inserted')

#------------------------------------
cursorEdu.execute( '''select 
                                stg.client_id, 
                                stg.last_name,
                                stg.first_name,
                                stg.patronymic,
                                stg.date_of_birth,
                                stg.passport_num,
                                stg.passport_valid_to,
                                stg.phone,
                                stg.update_dt,
                                null
                        from de11an.kmdd_stg_clients stg
                        left join de11an.kmdd_dwh_dim_clients tgt
                        on stg.client_id = tgt.client_id
                        where tgt.client_id is null''' )

records = cursorEdu.fetchall()

cursorEdu.executemany('''insert into de11an.kmdd_dwh_dim_clients( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt )
                                VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )''', records )
connEdu.commit()

print('stg clients inserted')

cursorEdu.execute( 'select * from de11an.kmdd_stg_blacklist')
records = cursorEdu.fetchall()
blocked = []
for r in records:
    blocked.append(r[0])
cursorEdu.executemany('''insert into de11an.kmdd_dwh_fact_passport_blacklist values ( %s, %s )''', records)
print('blacklist inserted')
connEdu.commit()

cursorEdu.execute( 'select * from de11an.kmdd_stg_transactions')
records = cursorEdu.fetchall()
cursorEdu.executemany('''insert into de11an.kmdd_dwh_fact_transactions values ( %s, %s, %s, %s, %s, %s, %s )''', records)
connEdu.commit()

print('transactions inserted')



cursorBank.execute( '''select distinct* from info.clients left join (select * from info.cards left join info.accounts on info.cards.account = info.accounts.account) tt
                                on info.clients.client_id = tt.client''')
records = cursorBank.fetchall()
names = [ x[0] for x in cursorBank.description ]
df = pd.DataFrame( records, columns = names )

#УБИРАЕМ ЛИШНИЙ ПРОБЕЛ)))))
for i in range(len(df.client_id)):
    old_value = df.iloc[i].card_num
    new_value = df.iloc[i].card_num[:-1]
    df['card_num'] = df['card_num'].replace([old_value], new_value)


report = pd.DataFrame(columns=['event_dt', 'passport', 'fio', 'phone', 'event_type', 'report_dt'])

cursorEdu.execute( '''select * from de11an.kmdd_stg_transactions''')
records = cursorEdu.fetchall()
names = [ x[0] for x in cursorEdu.description ]
dfTransactions = pd.DataFrame( records, columns = names )


new = dfTransactions.merge(df, how = 'left', left_on='card_num', right_on='card_num')


for i in range(len(new.trans_id)):
    if (new.iloc[i].passport_valid_to != None and not correctDate(new.iloc[i].trans_date.strftime('%Y-%m-%d'), new.iloc[i].passport_valid_to.strftime('%Y-%m-%d'))) or new.iloc[i].passport_num in blocked:
        report.loc[len(report.index)] = [new.iloc[i].trans_date, new.iloc[i].passport_num, new.iloc[i].last_name + new.iloc[i].first_name + new.iloc[i].patronymic,
                                         new.iloc[i].phone, 'Совершение операции при просроченном или заблокированном паспорте', '2021-03-03']

    if  not correctDate(new.iloc[i].trans_date.strftime('%Y-%m-%d'), new.iloc[i].valid_to.strftime('%Y-%m-%d')):
        report.loc[len(report.index)] = [new.iloc[i].trans_date, new.iloc[i].passport_num, new.iloc[i].last_name + new.iloc[i].first_name + new.iloc[i].patronymic,
                                         new.iloc[i].phone, 'Совершение операции при недействующем договоре', '2021-03-03']

print(report.head())


cursorEdu.executemany( "INSERT INTO de11an.kmdd_rep_fruad( 'event_dt', 'passport', 'fio', 'phone', 'event_type', 'report_dt' ) VALUES( %s, %s, %s, %s, %s, %s )", report.values.tolist() )
connEdu.commit()

os.rename('/home/de11an/kmdd/project/transactions_01032021.txt', '/home/de11an/kmdd/project/archive/transactions_01032021.txt.backup')
os.rename('/home/de11an/kmdd/project/terminals_01032021.txt', '/home/de11an/kmdd/project/archive/terminals_01032021.txt.backup')
os.rename('/home/de11an/kmdd/project/passport_blacklist_01032021.txt', '/home/de11an/kmdd/project/archive/passport_blacklist_01032021.txt.backup')

cursorEdu.close()
connEdu.close()

cursorBank.close()
connBank.close()
