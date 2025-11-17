# Guia Rápido - Geração de Data-Schemas

## 🚀 Início Rápido

### 1. Execute o Notebook (Mais Fácil)

```bash
# Abra no Jupyter/Zeppelin:
scripts/generate_data_schemas.ipynb

# Execute todas as células (Ctrl+Shift+Enter em cada célula)
```

### 2. Ou execute o script Python

```python
# No notebook ou console Python com Spark:
exec(open('scripts/generate_data_schemas.py').read())
```

## 📊 Comandos Individuais

Se preferir executar comando por comando:

### Obter DESCRIBE FORMATTED

```python
# Para uma tabela específica
spark = session.sparkSession
table_name = "fisca_fiscalizacoes_consolidadas"

df_desc = spark.sql(f"DESCRIBE FORMATTED teste.{table_name}")
df_desc.show(1000, truncate=False)

# Salvar resultado
df_desc.toPandas().to_csv(f"{table_name}_describe.csv", index=False)
```

### Obter SELECT * LIMIT 10

```python
# Para uma tabela específica
table_name = "fisca_fiscalizacoes_consolidadas"

df_sample = spark.sql(f"SELECT * FROM teste.{table_name} LIMIT 10")
df_sample.show(truncate=False)

# Salvar resultado
df_sample.toPandas().to_csv(f"{table_name}_sample.csv", index=False)
```

### Executar para todas as tabelas (One-liner)

```python
tables = ["fisca_fiscalizacoes_consolidadas", "fisca_dashboard_executivo",
          "fisca_scores_efetividade", "fisca_metricas_por_afre", "fisca_acompanhamentos"]

for t in tables:
    print(f"Processing {t}...")
    spark.sql(f"DESCRIBE FORMATTED teste.{t}").toPandas().to_csv(f"{t}_desc.csv", index=False)
    spark.sql(f"SELECT * FROM teste.{t} LIMIT 10").toPandas().to_csv(f"{t}_sample.csv", index=False)
    print(f"✓ {t} done!")
```

## 📝 Lista de Tabelas

### Tabelas do Banco (5)

```
teste.fisca_fiscalizacoes_consolidadas  ← Principal
teste.fisca_dashboard_executivo
teste.fisca_scores_efetividade
teste.fisca_metricas_por_afre
teste.fisca_acompanhamentos
```

### DataFrames Intermediários (10)

```
metrics_df           - KPIs dashboard
temporal_df          - Séries temporais
gerencia_df          - Por gerência
afre_df              - Por auditor
geo_df               - Geográfico
cnae_df              - Setorial
ml_df                - Machine Learning
network_df           - Análise redes
infraction_types_df  - Tipos infrações
company_details_df   - Detalhes empresa
```

## 🔍 Verificações Úteis

### Ver se tabela existe

```python
spark.sql("SHOW TABLES IN teste").show()
```

### Contar registros

```python
spark.sql("SELECT COUNT(*) FROM teste.fisca_fiscalizacoes_consolidadas").show()
```

### Ver primeiras colunas

```python
spark.sql("DESCRIBE teste.fisca_fiscalizacoes_consolidadas").show(20)
```

### Ver databases disponíveis

```python
spark.sql("SHOW DATABASES").show()
```

## 📁 Estrutura de Saída

Após executar, você terá:

```
data-schemas/
├── fisca_fiscalizacoes_consolidadas_describe_formatted.txt
├── fisca_fiscalizacoes_consolidadas_select_limit_10.txt
├── fisca_fiscalizacoes_consolidadas_sample_data.csv
├── fisca_dashboard_executivo_describe_formatted.txt
├── fisca_dashboard_executivo_select_limit_10.txt
├── fisca_dashboard_executivo_sample_data.csv
├── ...
├── intermediate_tables_README.txt
└── SUMMARY_REPORT.txt
```

## ⚡ Comandos Super Rápidos

### Comando único para gerar tudo

```python
# Copy-paste isso no notebook:
import os
from pathlib import Path
from datetime import datetime

spark = session.sparkSession
output_dir = Path("data-schemas")
output_dir.mkdir(exist_ok=True)

tables = ["fisca_fiscalizacoes_consolidadas", "fisca_dashboard_executivo",
          "fisca_scores_efetividade", "fisca_metricas_por_afre", "fisca_acompanhamentos"]

for table in tables:
    print(f"\n{'='*60}")
    print(f"📊 {table}")
    print('='*60)

    # DESCRIBE
    desc = spark.sql(f"DESCRIBE FORMATTED teste.{table}").toPandas()
    desc_file = output_dir / f"{table}_describe_formatted.txt"
    desc.to_string(desc_file, index=False)
    print(f"✓ DESCRIBE saved to {desc_file.name}")

    # SELECT
    sample = spark.sql(f"SELECT * FROM teste.{table} LIMIT 10").toPandas()
    sample_file = output_dir / f"{table}_select_limit_10.txt"
    sample.to_string(sample_file, index=False)
    csv_file = output_dir / f"{table}_sample_data.csv"
    sample.to_csv(csv_file, index=False)
    print(f"✓ SELECT saved to {sample_file.name} and {csv_file.name}")

print(f"\n\n✅ Done! Check {output_dir.absolute()}")
```

## 🐛 Troubleshooting

### Erro: SparkSession não encontrado

```python
# Certifique-se de estar em ambiente notebook com Spark
# Teste:
try:
    spark = session.sparkSession
    print("✓ SparkSession OK")
except:
    print("✗ Rode este código em um notebook Jupyter/Zeppelin com Spark")
```

### Erro: Table not found

```python
# Verifique se a tabela existe:
spark.sql("SHOW TABLES IN teste").show()

# Ou tente com outro database:
spark.sql("SHOW DATABASES").show()
```

### Query muito lenta

```python
# Reduza o LIMIT:
spark.sql("SELECT * FROM teste.table_name LIMIT 5")

# Ou selecione apenas algumas colunas:
spark.sql("SELECT col1, col2, col3 FROM teste.table_name LIMIT 10")
```

## 📞 Ajuda

- README completo: `scripts/README_data_schemas.md`
- Exemplos PySpark: `scripts/exemplo_comandos_pyspark.py`
- Notebook completo: `scripts/generate_data_schemas.ipynb`

---

**Dica:** Copie e cole os comandos diretamente no notebook!
