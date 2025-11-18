#!/usr/bin/env python3
"""
Script para gerar data-schemas das tabelas FISCA
Gera DESCRIBE FORMATTED e SELECT * LIMIT 10 para cada tabela

Uso:
    python generate_data_schemas.py

Requer:
    - Acesso ao cluster Spark/Impala
    - session.sparkSession disponível (ambiente notebook)
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Configurações
DATABASE = "teste"
OUTPUT_DIR = "data-schemas"

# Lista de tabelas originais do banco
ORIGINAL_TABLES = [
    "fisca_fiscalizacoes_consolidadas",
    "fisca_dashboard_executivo",
    "fisca_scores_efetividade",
    "fisca_metricas_por_afre",
    "fisca_acompanhamentos",
]

# Lista de tabelas intermediárias (DataFrames gerados pelo código)
INTERMEDIATE_TABLES = {
    "metrics_df": "Dashboard KPIs - métricas consolidadas",
    "temporal_df": "Análise temporal - séries temporais",
    "gerencia_df": "Performance por gerência",
    "afre_df": "Performance por auditor fiscal",
    "geo_df": "Distribuição geográfica",
    "cnae_df": "Análise setorial por CNAE",
    "ml_df": "Dataset para machine learning",
    "network_df": "Relacionamentos empresa-auditor-infração",
    "infraction_types_df": "Classificação de tipos de infrações",
    "company_details_df": "Detalhes de empresas por CNPJ",
}


def create_output_directory():
    """Cria diretório de saída para os schemas"""
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(exist_ok=True)
    print(f"📁 Diretório de saída: {output_path.absolute()}")
    return output_path


def generate_describe_query(table_name: str, database: str = DATABASE) -> str:
    """Gera query DESCRIBE FORMATTED"""
    return f"DESCRIBE FORMATTED {database}.{table_name}"


def generate_select_query(table_name: str, database: str = DATABASE, limit: int = 10) -> str:
    """Gera query SELECT * LIMIT"""
    return f"SELECT * FROM {database}.{table_name} LIMIT {limit}"


def save_query_result_to_file(output_path: Path, table_name: str, query_type: str, content: str):
    """Salva resultado da query em arquivo"""
    filename = f"{table_name}_{query_type}.txt"
    filepath = output_path / filename

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"# {table_name} - {query_type}\n")
        f.write(f"# Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# {'='*80}\n\n")
        f.write(content)

    print(f"   ✓ Salvo: {filename}")


def execute_and_save_queries(spark, table_name: str, output_path: Path, database: str = DATABASE):
    """
    Executa DESCRIBE FORMATTED e SELECT LIMIT 10 para uma tabela
    e salva os resultados
    """
    print(f"\n{'='*80}")
    print(f"📊 Processando tabela: {database}.{table_name}")
    print(f"{'='*80}")

    try:
        # 1. DESCRIBE FORMATTED
        print("\n1️⃣ Executando DESCRIBE FORMATTED...")
        describe_query = generate_describe_query(table_name, database)
        print(f"   Query: {describe_query}")

        df_describe = spark.sql(describe_query)
        describe_output = df_describe.toPandas().to_string(index=False)
        save_query_result_to_file(output_path, table_name, "describe_formatted", describe_output)

        # 2. SELECT * LIMIT 10
        print("\n2️⃣ Executando SELECT * LIMIT 10...")
        select_query = generate_select_query(table_name, database, limit=10)
        print(f"   Query: {select_query}")

        df_select = spark.sql(select_query)

        # Salva em formato texto
        select_output = df_select.toPandas().to_string(index=False)
        save_query_result_to_file(output_path, table_name, "select_limit_10", select_output)

        # Também salva em CSV para facilitar análise
        csv_filename = f"{table_name}_sample_data.csv"
        csv_filepath = output_path / csv_filename
        df_select.toPandas().to_csv(csv_filepath, index=False, encoding='utf-8')
        print(f"   ✓ Salvo CSV: {csv_filename}")

        print(f"\n✅ Tabela {table_name} processada com sucesso!")

    except Exception as e:
        print(f"\n❌ Erro ao processar {table_name}: {str(e)}")
        # Salva erro em arquivo
        error_filename = f"{table_name}_ERROR.txt"
        error_filepath = output_path / error_filename
        with open(error_filepath, 'w', encoding='utf-8') as f:
            f.write(f"Erro ao processar {table_name}\n")
            f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Erro: {str(e)}\n")


def generate_intermediate_table_docs(output_path: Path):
    """
    Gera documentação para as tabelas intermediárias (DataFrames)
    """
    print(f"\n{'='*80}")
    print("📝 Gerando documentação para tabelas intermediárias...")
    print(f"{'='*80}\n")

    doc_content = "# TABELAS INTERMEDIÁRIAS (DataFrames in-memory)\n\n"
    doc_content += f"Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    doc_content += "Estas são tabelas criadas dinamicamente como DataFrames Pandas.\n"
    doc_content += "Não são tabelas persistentes no banco de dados.\n\n"
    doc_content += f"{'='*80}\n\n"

    for table_name, description in INTERMEDIATE_TABLES.items():
        doc_content += f"## {table_name}\n"
        doc_content += f"**Descrição:** {description}\n"
        doc_content += f"**Tipo:** DataFrame Pandas (in-memory)\n"
        doc_content += f"**Origem:** Derivado de fisca_fiscalizacoes_consolidadas\n\n"
        doc_content += "**Estrutura:** Para obter a estrutura, execute a query correspondente\n"
        doc_content += "no arquivo src/modules/database.py\n\n"
        doc_content += f"{'-'*80}\n\n"

    doc_filepath = output_path / "intermediate_tables_README.txt"
    with open(doc_filepath, 'w', encoding='utf-8') as f:
        f.write(doc_content)

    print(f"✓ Documentação salva: intermediate_tables_README.txt")


def generate_summary_report(output_path: Path, processed_tables: list, failed_tables: list):
    """Gera relatório resumo da execução"""
    print(f"\n{'='*80}")
    print("📋 Gerando relatório resumo...")
    print(f"{'='*80}\n")

    summary = "# RELATÓRIO DE GERAÇÃO DE DATA-SCHEMAS\n\n"
    summary += f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    summary += f"Database: {DATABASE}\n\n"

    summary += f"## RESUMO\n\n"
    summary += f"- Total de tabelas processadas: {len(processed_tables)}\n"
    summary += f"- Tabelas com sucesso: {len(processed_tables) - len(failed_tables)}\n"
    summary += f"- Tabelas com erro: {len(failed_tables)}\n\n"

    summary += f"## TABELAS PROCESSADAS COM SUCESSO\n\n"
    for table in processed_tables:
        if table not in failed_tables:
            summary += f"✓ {table}\n"

    if failed_tables:
        summary += f"\n## TABELAS COM ERRO\n\n"
        for table in failed_tables:
            summary += f"✗ {table}\n"

    summary += f"\n## ARQUIVOS GERADOS\n\n"
    summary += "Para cada tabela foram gerados:\n"
    summary += "- {table_name}_describe_formatted.txt - Schema detalhado da tabela\n"
    summary += "- {table_name}_select_limit_10.txt - Primeiras 10 linhas em formato texto\n"
    summary += "- {table_name}_sample_data.csv - Primeiras 10 linhas em formato CSV\n\n"

    summary += "## PRÓXIMOS PASSOS\n\n"
    summary += "1. Revise os arquivos gerados em: " + str(output_path.absolute()) + "\n"
    summary += "2. Use os schemas para documentar o projeto\n"
    summary += "3. Valide a estrutura das tabelas\n"

    report_filepath = output_path / "SUMMARY_REPORT.txt"
    with open(report_filepath, 'w', encoding='utf-8') as f:
        f.write(summary)

    print(f"✓ Relatório salvo: SUMMARY_REPORT.txt")


def main():
    """Função principal"""
    print("\n" + "="*80)
    print("🚀 GERADOR DE DATA-SCHEMAS FISCA")
    print("="*80 + "\n")

    # Verifica se está em ambiente Spark
    try:
        # Tenta obter SparkSession do ambiente notebook
        spark = session.sparkSession  # type: ignore
        print("✓ SparkSession obtida com sucesso!")
    except NameError:
        print("❌ ERRO: SparkSession não encontrada!")
        print("\nEste script deve ser executado em um ambiente Jupyter/Zeppelin")
        print("com acesso ao session.sparkSession")
        print("\nAlternativamente, descomente o código abaixo para criar uma SparkSession local")
        sys.exit(1)

    # Cria diretório de saída
    output_path = create_output_directory()

    # Lista para rastrear processamento
    processed_tables = []
    failed_tables = []

    # Processa cada tabela original
    print(f"\n{'='*80}")
    print(f"📊 PROCESSANDO TABELAS ORIGINAIS ({len(ORIGINAL_TABLES)} tabelas)")
    print(f"{'='*80}")

    for table in ORIGINAL_TABLES:
        processed_tables.append(table)
        try:
            execute_and_save_queries(spark, table, output_path, DATABASE)
        except Exception as e:
            print(f"❌ Falha crítica ao processar {table}: {e}")
            failed_tables.append(table)

    # Gera documentação das tabelas intermediárias
    generate_intermediate_table_docs(output_path)

    # Gera relatório resumo
    generate_summary_report(output_path, processed_tables, failed_tables)

    # Mensagem final
    print("\n" + "="*80)
    print("✅ PROCESSAMENTO CONCLUÍDO!")
    print("="*80 + "\n")
    print(f"📁 Todos os arquivos foram salvos em: {output_path.absolute()}")
    print(f"📊 Tabelas processadas: {len(processed_tables)}")
    print(f"✓ Sucesso: {len(processed_tables) - len(failed_tables)}")
    print(f"✗ Erros: {len(failed_tables)}")

    if failed_tables:
        print("\n⚠️ Atenção: Algumas tabelas falharam:")
        for table in failed_tables:
            print(f"   - {table}")

    print("\n" + "="*80 + "\n")


# ============================================================================
# CÓDIGO ALTERNATIVO: Para executar fora do ambiente notebook
# ============================================================================
#
# Descomente o código abaixo se quiser executar fora do notebook
# IMPORTANTE: Ajuste as configurações de conexão
#
# from pyspark.sql import SparkSession
#
# def create_spark_session():
#     """Cria SparkSession para execução standalone"""
#     spark = SparkSession.builder \
#         .appName("FISCA Data Schema Generator") \
#         .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
#         .enableHiveSupport() \
#         .getOrCreate()
#     return spark
#
# if __name__ == "__main__":
#     spark = create_spark_session()
#     # Execute o main() usando essa spark session


if __name__ == "__main__":
    main()
