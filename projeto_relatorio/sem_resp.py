import pandas as pd
import time
import mysql.connector
from datetime import datetime, timedelta

banco = mysql.connector.connect(
    host="mysql.nerusconsultoria.com.br",
    user="nerusconsultor02",
    passwd="0l1d3ranca2",
    database="nerusconsultor02"
)

cursor = banco.cursor()

hoje = datetime.today().weekday()

if hoje == 0:
    a = '3'
else:
    a = '1'

comando_sql_1 = '''SELECT cc.id,cc.empresa,op.operador,resp_ontem.resp_ontem FROM cadastro_clientes AS cc
                LEFT JOIN (SELECT CONCAT(cliente_id_fk) AS id,COUNT(*) AS resp_ontem FROM sales_base_leads
                            WHERE DATE_FORMAT(ultima_resposta,'%Y-%m-%d') = CURRENT_DATE - interval '''

comando_sql_2 = ''' day GROUP BY cliente_id_fk) 
                    AS resp_ontem ON resp_ontem.id = cc.id

            JOIN carteira_clientes AS op ON op.id = cc.operador_fk

            WHERE cc.status = 2 OR cc.status = 1 AND cc.campanha_pausada = 1'''

comando_sql_leitura = comando_sql_1 + a + comando_sql_2

cursor.execute(comando_sql_leitura)

valores_lidos = cursor.fetchall()

df_carteira = pd.DataFrame(valores_lidos)

df_carteira.columns = ['id', 'empresa', 'carteira', 'resp_ontem']

null_ontem = df_carteira.resp_ontem.isna().sum()

if hoje == 0:
    data_verif = datetime.today() - timedelta(3)
else:
    data_verif = datetime.today() - timedelta(1)

parte_1 = 'INSERT INTO sem_respostas_python (total_null, data_verif) VALUES ('

comando_sql_entrada = parte_1 + \
    str(null_ontem) + ', "' + data_verif.strftime('%Y-%m-%d') + '")'

cursor.execute(comando_sql_entrada)

banco.commit()
