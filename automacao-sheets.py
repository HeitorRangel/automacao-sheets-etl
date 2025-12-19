import gspread
import pandas as pd
import os
import logging
import sqlite3
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuração de Log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("processo_etl.log"), logging.StreamHandler()]
)

class VendasETL:
    def __init__(self):
        self.google_credencial = os.getenv('GOOGLE_CREDENTIALS_PATH')
        self.db_path = 'vendas_dw.db'
        self.colunas_obrigatorias = {'Data', 'Vendedor', 'Produto', 'Quantidade', 'Valor Unitario', 'Total'}
        self.client = self._conectar_google()

    def _conectar_google(self):
        """Método privado para conexão."""
        try:
            if not self.google_credencial:
                raise ValueError("Variável GOOGLE_CREDENTIALS_PATH não configurada.")
            return gspread.service_account(filename=self.google_credencial)
        except Exception as e:
            logging.critical(f'Falha na conexão com Google: {e}', exc_info=True)
            return None

    def validar_schema(self, df, nome_planilha):
        colunas_presentes = set(df.columns)
        if not self.colunas_obrigatorias.issubset(colunas_presentes):
            faltantes = self.colunas_obrigatorias - colunas_presentes
            logging.error(f'SCHEMA INVÁLIDO em "{nome_planilha}". Faltam: {faltantes}')
            return False
        return True

    def extract(self, pattern="vendas -"):
        """Busca dinâmica: Pega todas as planilhas que contêm o padrão no nome."""
        if not self.client: return []
        
        dfs = []
        # Lista todas as planilhas disponíveis na conta de serviço
        all_spreadsheets = self.client.openall()
        
        for spreadsheet in all_spreadsheets:
            if pattern in spreadsheet.title.lower():
                logging.info(f'Extraindo: {spreadsheet.title}')
                try:
                    worksheet = spreadsheet.get_worksheet(0)
                    data = worksheet.get_all_values()
                    df = pd.DataFrame(data[1:], columns=data[0])
                    
                    if self.validar_schema(df, spreadsheet.title):
                        dfs.append(df)
                except Exception as e:
                    logging.error(f'Erro ao ler {spreadsheet.title}: {e}', exc_info=True)
        return dfs

    def transform(self, df_list):
        if not df_list: return None
        
        logging.info('Iniciando Transformação...')
        combined_df = pd.concat(df_list, ignore_index=True)

        # Limpeza de números (remove R$ e espaços)
        cols_numericas = ['Valor Unitario', 'Total', 'Quantidade']
        for col in cols_numericas:
            if col in combined_df.columns:
                # Remove 'R$', espaços e troca vírgula por ponto
                combined_df[col] = combined_df[col].astype(str).apply(
                    lambda x: x.replace('R$', '').replace('.', '').replace(',', '.').strip()
                )
                combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')

        if 'Data' in combined_df.columns:
            combined_df['Data'] = pd.to_datetime(combined_df['Data'], errors='coerce', dayfirst=True)
            
        return combined_df

    def load(self, df):
        if df is None or df.empty:
            logging.warning("Nenhum dado para salvar.")
            return

        try:
            # Context Manager (with) garante o fechamento do banco
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql('tb_vendas_consolidadas', conn, if_exists='replace', index=False)
                logging.info(f'Carga concluída! {len(df)} registros inseridos no Data Warehouse.')
        except Exception as e:
            logging.error(f'Erro no Load: {e}', exc_info=True)

    def run(self):
        logging.info("Iniciando Pipeline ETL...")
        dados_brutos = self.extract()
        dados_tratados = self.transform(dados_brutos)
        self.load(dados_tratados)

if __name__ == '__main__':
    pipeline = VendasETL()
    pipeline.run()