import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# ETAPA 1: EXTRAÇÃO
# Suponhamos que temos um arquivo CSV com dados de vendas
# O arquivo CSV possui colunas: 'id', 'data', 'produto', 'quantidade', 'preco_unitario'

# Carregar o arquivo CSV
df = pd.read_csv("vendas.csv")

# Exibir as primeiras linhas para inspeção
print("Dados extraídos:")
print(df.head())

# ETAPA 2: TRANSFORMAÇÃO
# Limpeza e transformação dos dados
# 1. Converter a coluna 'data' para o formato de data
df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d')

# 2. Calcular o valor total da venda (quantidade * preço_unitario)
df['valor_total'] = df['quantidade'] * df['preco_unitario']

# 3. Remover linhas com valores ausentes
df.dropna(subset=['produto', 'quantidade', 'preco_unitario'], inplace=True)

# 4. Agregar as vendas por produto (soma do valor total e quantidade)
df_aggregated = df.groupby('produto').agg(
    total_vendas=('valor_total', 'sum'),
    quantidade_total=('quantidade', 'sum')
).reset_index()

# Exibir os dados transformados
print("\nDados transformados e agregados:")
print(df_aggregated)

# ETAPA 3: CARGA
# Conectar ao banco de dados MySQL
# Altere os parâmetros de conexão conforme sua configuração
db_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='sua_senha',
    database='vendas_db'
)

# Usando SQLAlchemy para facilitar a carga
engine = create_engine('mysql+mysqlconnector://root:sua_senha@localhost/vendas_db')

# Carregar os dados agregados na tabela 'vendas_aggregated'
df_aggregated.to_sql('vendas_aggregated', con=engine, if_exists='replace', index=False)

print("\nDados carregados com sucesso para o banco de dados MySQL.")
