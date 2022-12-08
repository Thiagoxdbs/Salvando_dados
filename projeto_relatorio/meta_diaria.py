import pandas as pd
import time
import mysql.connector
from datetime import datetime, timedelta
import numpy as np


def SQL_INSERT_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET):
    sql_texts = []
    for index, row in SOURCE.iterrows():
        sql_texts.append('INSERT INTO '+TARGET+' (' +
                         str(', '.join(SOURCE.columns)) + ') VALUES ' + str(tuple(row.values)))
    return sql_texts


def dias_mes():
    mes_ = int(datetime.today().month)
    match mes_:
        case 1:
            return 31
        case 2:
            return 28
        case 3:
            return 31
        case 4:
            return 30
        case 5:
            return 31
        case 6:
            return 30
        case 7:
            return 31
        case 8:
            return 31
        case 9:
            return 30
        case 10:
            return 31
        case 11:
            return 30
        case 12:
            return 31


mes = dias_mes()

banco = mysql.connector.connect(
    host="mysql.nerusconsultoria.com.br",
    user="nerusconsultor02",
    passwd="0l1d3ranca2",
    database="nerusconsultor02"
)

cursor = banco.cursor()

comando_sql = '''SELECT ca.id AS ID,ca.empresa AS Cliente,cc.operador,ca.meta_num,int_hj.interessados FROM nerusconsultor02.cadastro_clientes AS ca
                LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS interessados FROM sales_base_leads 
			    WHERE EXTRACT(YEAR_MONTH FROM ultima_resposta) = EXTRACT(YEAR_MONTH FROM CURRENT_TIMESTAMP) 
                AND status_interested_id_level_fk = 1 GROUP BY cliente_id_fk) AS int_hj ON int_hj.id = ca.id
                JOIN carteira_clientes AS cc ON cc.id = ca.operador_fk 
                WHERE ca.status != 0 AND ca.campanha_pausada = 1 AND ca.id != 20 AND ca.id !=22'''

cursor.execute(comando_sql)

valores_lidos = cursor.fetchall()

df_base = pd.DataFrame(valores_lidos)

df_base = df_base.rename(
    columns={0: "id", 1: "empresa", 2: "carteira", 3: "meta_num", 4: "int_hj"})

hoje = datetime.today().weekday()

if hoje == 0:
    data_verif = datetime.today() - timedelta(3)
else:
    data_verif = datetime.today() - timedelta(1)

hj = int(datetime.today().strftime('%d'))
tx = 1/mes
tx_hj = hj*tx

df_base['tx_hj'] = tx_hj

df_base['meta_hj'] = round(df_base['meta_num']*df_base['tx_hj'])

df_base['result'] = np.where(df_base['int_hj'] >= df_base['meta_hj'], 1, 0)

df_base['data_verif'] = data_verif.strftime('%Y-%m-%d')

df_entrada = df_base[['id', 'result', 'data_verif']]

comando_sql = SQL_INSERT_STATEMENT_FROM_DATAFRAME(
    df_entrada, "meta_diaria_python")

for st in comando_sql:
    cursor.execute(st)

banco.commit()
