"""
Exemplos de Comandos PySpark SQL para FISCA
============================================

Este arquivo contém exemplos de comandos PySpark SQL baseados nos notebooks
existentes do projeto FISCA.

Para executar estes comandos, você precisa estar em um ambiente Jupyter/Zeppelin
com acesso ao SparkSession.
"""

# ============================================================================
# 1. SETUP INICIAL
# ============================================================================

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date

# Acesso ao SparkSession (disponível no ambiente notebook)
spark = session.sparkSession  # type: ignore


# ============================================================================
# 2. COMANDOS BÁSICOS
# ============================================================================

# Listar databases disponíveis
spark.sql("SHOW DATABASES").show(truncate=False)

# Listar tabelas de um database
spark.sql("SHOW TABLES IN teste").show(truncate=False)

# Ver estrutura de uma tabela
spark.sql("DESCRIBE teste.fisca_fiscalizacoes_consolidadas").show(truncate=False)

# Ver estrutura detalhada (DESCRIBE FORMATTED)
spark.sql("DESCRIBE FORMATTED teste.fisca_fiscalizacoes_consolidadas").show(truncate=False)


# ============================================================================
# 3. QUERIES SIMPLES
# ============================================================================

# Select simples com limit
df = spark.sql("""
    SELECT *
    FROM teste.fisca_fiscalizacoes_consolidadas
    LIMIT 10
""")
df.show()

# Converter para Pandas
df_pandas = df.toPandas()
print(df_pandas.head())


# ============================================================================
# 4. QUERIES COM AGREGAÇÕES
# ============================================================================

# Contar registros
total = spark.sql("""
    SELECT COUNT(*) as total
    FROM teste.fisca_fiscalizacoes_consolidadas
""").collect()[0]['total']
print(f"Total de registros: {total:,}")

# Agregações por ano
df_ano = spark.sql("""
    SELECT
        YEAR(data_infracao) as ano,
        COUNT(*) as qtd_infracoes,
        SUM(valor_infracao) as valor_total
    FROM teste.fisca_fiscalizacoes_consolidadas
    WHERE data_infracao IS NOT NULL
    GROUP BY YEAR(data_infracao)
    ORDER BY ano DESC
""")
df_ano.show()


# ============================================================================
# 5. CRIAR VIEW TEMPORÁRIA
# ============================================================================

# Criar view temporária para usar em múltiplas queries
query = """
    SELECT
        *,
        YEAR(data_infracao) as ano_infracao,
        MONTH(data_infracao) as mes_infracao
    FROM teste.fisca_fiscalizacoes_consolidadas
    WHERE ano_infracao >= 2020
"""

spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW vw_infracoes AS {query}")

# Usar a view
spark.sql("SELECT * FROM vw_infracoes LIMIT 5").show()


# ============================================================================
# 6. QUERIES COMPLEXAS COM CAST
# ============================================================================

# Query com conversão de tipos (padrão usado nos notebooks)
df_metricas = spark.sql("""
    SELECT
        CAST(COALESCE(total_infracoes, 0) AS BIGINT) AS total_infracoes,
        CAST(COALESCE(total_empresas, 0) AS BIGINT) AS total_empresas,
        CAST(COALESCE(valor_total, 0) AS DOUBLE) AS valor_total,
        CAST(COALESCE(taxa_conversao, 0) AS DOUBLE) AS taxa_conversao
    FROM teste.fisca_dashboard_executivo
    ORDER BY ano DESC
""")
df_metricas.show()


# ============================================================================
# 7. JOINS ENTRE TABELAS
# ============================================================================

# Exemplo de JOIN (se houver múltiplas tabelas)
df_join = spark.sql("""
    SELECT
        a.*,
        f.razao_social,
        f.valor_infracao
    FROM teste.fisca_acompanhamentos a
    LEFT JOIN teste.fisca_fiscalizacoes_consolidadas f
        ON a.cnpj = f.cnpj
    WHERE a.ano_os >= 2020
    LIMIT 10
""")
df_join.show()


# ============================================================================
# 8. FUNÇÕES ÚTEIS
# ============================================================================

def query_to_pandas(query: str, limit: int = None) -> pd.DataFrame:
    """
    Executa query Spark SQL e retorna Pandas DataFrame

    Args:
        query: Query SQL
        limit: Limite de registros (opcional)

    Returns:
        DataFrame Pandas
    """
    if limit:
        query = f"{query} LIMIT {limit}"

    df_spark = spark.sql(query)
    return df_spark.toPandas()


def describe_table(table_name: str, database: str = "teste") -> pd.DataFrame:
    """
    Retorna DESCRIBE FORMATTED de uma tabela

    Args:
        table_name: Nome da tabela
        database: Nome do database

    Returns:
        DataFrame com descrição da tabela
    """
    query = f"DESCRIBE FORMATTED {database}.{table_name}"
    return spark.sql(query).toPandas()


def sample_table(table_name: str, database: str = "teste", n: int = 10) -> pd.DataFrame:
    """
    Retorna amostra de registros de uma tabela

    Args:
        table_name: Nome da tabela
        database: Nome do database
        n: Número de registros

    Returns:
        DataFrame com amostra
    """
    query = f"SELECT * FROM {database}.{table_name} LIMIT {n}"
    return spark.sql(query).toPandas()


# ============================================================================
# 9. EXEMPLOS DE USO DAS FUNÇÕES
# ============================================================================

if __name__ == "__main__":
    # Exemplo 1: Obter schema de uma tabela
    schema_df = describe_table("fisca_fiscalizacoes_consolidadas")
    print("\n=== SCHEMA ===")
    print(schema_df.to_string())

    # Exemplo 2: Obter amostra de dados
    sample_df = sample_table("fisca_fiscalizacoes_consolidadas", n=5)
    print("\n=== AMOSTRA DE DADOS ===")
    print(sample_df.to_string())

    # Exemplo 3: Query customizada
    custom_query = """
        SELECT
            gerencia,
            COUNT(*) as total,
            SUM(valor_infracao) as valor_total
        FROM teste.fisca_fiscalizacoes_consolidadas
        GROUP BY gerencia
        ORDER BY valor_total DESC
    """
    result_df = query_to_pandas(custom_query, limit=10)
    print("\n=== QUERY CUSTOMIZADA ===")
    print(result_df.to_string())


# ============================================================================
# 10. DICAS E BOAS PRÁTICAS
# ============================================================================

"""
DICAS:

1. Use CAST para garantir tipos corretos:
   CAST(column AS BIGINT), CAST(column AS DOUBLE), etc.

2. Use COALESCE para tratar NULLs:
   COALESCE(column, 0) AS column

3. Para grandes volumes, use LIMIT durante desenvolvimento:
   SELECT * FROM table LIMIT 1000

4. Cache DataFrames que serão reutilizados:
   df.cache()
   # ... usar df várias vezes ...
   df.unpersist()

5. Prefira criar views temporárias para queries complexas:
   CREATE OR REPLACE TEMPORARY VIEW vw_name AS SELECT...

6. Use .toPandas() com cuidado em grandes datasets:
   - Pode causar OutOfMemory
   - Sempre use LIMIT quando possível

7. Para queries muito grandes, processe por partições:
   spark.sql(query).repartition(10).write.parquet("output/")

8. Monitore o processamento:
   total = spark.sql("SELECT COUNT(*) FROM table").collect()[0][0]
   print(f"Processando {total:,} registros...")

9. Use formatação de números brasileira:
   f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

10. Sempre feche conexões e limpe cache:
    df.unpersist()
    spark.catalog.clearCache()
"""


# ============================================================================
# 11. TROUBLESHOOTING
# ============================================================================

"""
ERROS COMUNS:

1. "Table not found"
   Solução: Verificar se a tabela existe
   spark.sql("SHOW TABLES IN teste").show()

2. "Column not found"
   Solução: Verificar colunas disponíveis
   spark.sql("DESCRIBE teste.table_name").show()

3. "OutOfMemoryError"
   Solução: Usar LIMIT ou processar em lotes

4. "AnalysisException: cannot resolve 'column'"
   Solução: Verificar tipo de dado e fazer CAST se necessário

5. Query muito lenta
   Solução:
   - Adicionar filtros WHERE
   - Reduzir colunas no SELECT
   - Usar particionamento
"""
