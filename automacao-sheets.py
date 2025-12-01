import gspread
import pandas as pd
import os
import logging  
from datetime import datetime
import dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("processo_etl.log"),
        logging.StreamHandler()
    ])

google_credencial = os.getenv('GOOGLE_CREDENTIALS_PATH')
output = os.getenv('NOME_ARQUIVO_SAIDA', 'dashboard.xlsx')

sheet_name = ['vendas - setembro', 'vendas - outubro', 'vendas - novembro']
sheet_index = 0

def lerPlanilhas():
    all_df = []

    if not google_credencial:
        logging.critical('Caminho para as credenciais do Google nao encontrado. Verifique o arquivo .env.')
        return None

    logging.info('Iniciando processo ETL...')
    logging.info('Conectando ao Google Sheets...')

    try:
        gc = gspread.service_account(filename=google_credencial)
    except Exception as e:
        logging.error(f'Erro ao conectar ao Google Sheets: {e}', exec_info=True)
        return None
    
    #FASE 1: COLETA DE DADOS
    for name in sheet_name:
        logging.info(f'Lendo planilha: {name}')
        try:
            abrir_planilha = gc.open(name)
            worksheet = abrir_planilha.get_worksheet(sheet_index)
            data = worksheet.get_all_values()
            
            # Cria o DataFrame individual
            df = pd.DataFrame(data[1:], columns=data[0])
            all_df.append(df)
            
        except gspread.exceptions.SpreadsheetNotFound:
            logging.warning(f'Planilha {name} nao encontrada. Passando para a proxima.')
        except Exception as e:
            logging.error(f'Erro ao ler a planilha {name}: {e}', exc_info=True)

    #FASE 2: PROCESSAMENTO E SAÍDA    
    if not all_df:
        logging.critical('Nenhuma planilha foi lida com sucesso. Encerrando o processo ETL.')
        return None
        
    logging.info('Consolidando dados...')
    combined_df = pd.concat(all_df, ignore_index=True)

    # Tratamento de dados (conversão de moeda e datas)
    for col in ['Valor Unitario', 'Total', 'Quantidade']:
        # Verifica se a coluna existe antes de tentar converter para evitar erros
        if col in combined_df.columns:
            combined_df[col] = pd.to_numeric(
                combined_df[col].astype(str).str.replace(',', '.', regex=False), 
                errors='coerce'
            )

    if 'Data' in combined_df.columns:
        combined_df['Data'] = pd.to_datetime(combined_df['Data'], errors='coerce', dayfirst=True)

    try:
        combined_df.to_excel(output, index=False)
        logging.info(f'Processo ETL concluído com sucesso. Dados salvos em {output}, as {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    except Exception as e:
        logging.error(f'Erro ao salvar o arquivo Excel: {e}', exc_info=True)

    return combined_df
    
if __name__ == '__main__':
    lerPlanilhas()