# Gerador de Data-Schemas FISCA

Scripts para gerar automaticamente os arquivos de schema (`DESCRIBE FORMATTED` e `SELECT * LIMIT 10`) para todas as tabelas do projeto FISCA.

## 📋 Tabelas que serão processadas

### Tabelas Originais (5)
Tabelas persistentes no banco de dados `teste`:

1. `fisca_fiscalizacoes_consolidadas` - Tabela principal consolidada
2. `fisca_dashboard_executivo` - Dashboard executivo
3. `fisca_scores_efetividade` - Scores de efetividade
4. `fisca_metricas_por_afre` - Métricas por auditor fiscal
5. `fisca_acompanhamentos` - Dados de acompanhamentos

### Tabelas Intermediárias (10)
DataFrames Pandas criados dinamicamente (in-memory):

1. `metrics_df` - KPIs do dashboard
2. `temporal_df` - Análise temporal
3. `gerencia_df` - Performance por gerência
4. `afre_df` - Performance por auditor
5. `geo_df` - Distribuição geográfica
6. `cnae_df` - Análise setorial
7. `ml_df` - Dataset ML
8. `network_df` - Análise de redes
9. `infraction_types_df` - Tipos de infrações
10. `company_details_df` - Detalhes de empresas

## 🚀 Como Usar

### Opção 1: Jupyter Notebook (Recomendado)

```bash
# 1. Abra o notebook no ambiente Jupyter/Zeppelin
scripts/generate_data_schemas.ipynb

# 2. Execute todas as células
# O script vai:
# - Conectar ao banco via SparkSession
# - Executar queries para cada tabela
# - Salvar resultados em data-schemas/
```

### Opção 2: Script Python

```bash
# No ambiente com PySpark configurado:
cd /home/user/Fisca
python scripts/generate_data_schemas.py
```

**Nota:** O script Python requer que `session.sparkSession` esteja disponível (ambiente notebook).

### Opção 3: Execução Standalone

Se quiser executar fora do notebook, edite o arquivo `generate_data_schemas.py` e descomente a seção:

```python
# ============================================================================
# CÓDIGO ALTERNATIVO: Para executar fora do ambiente notebook
# ============================================================================
```

## 📁 Arquivos Gerados

Após a execução, será criado o diretório `data-schemas/` contendo:

### Para cada tabela original:
```
{nome_tabela}_describe_formatted.txt    # Schema completo da tabela
{nome_tabela}_select_limit_10.txt       # Primeiras 10 linhas (formato texto)
{nome_tabela}_sample_data.csv           # Primeiras 10 linhas (formato CSV)
```

### Documentação adicional:
```
intermediate_tables_README.txt          # Documentação das tabelas intermediárias
SUMMARY_REPORT.txt                      # Relatório resumo da execução
```

## 📊 Exemplo de Saída

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

## ⚙️ Configuração

### Variáveis configuráveis:

```python
DATABASE = "teste"              # Nome do database no Impala
OUTPUT_DIR = "data-schemas"     # Diretório de saída
```

### Para adicionar novas tabelas:

Edite a lista `ORIGINAL_TABLES` no script:

```python
ORIGINAL_TABLES = [
    "fisca_fiscalizacoes_consolidadas",
    "sua_nova_tabela",  # Adicione aqui
]
```

## 🔧 Requisitos

- Python 3.7+
- PySpark / SparkSession configurado
- Acesso ao banco de dados Impala (cluster configurado)
- Bibliotecas: `pyspark`, `pandas`

## 📝 Notas Importantes

1. **Permissões**: Certifique-se de ter permissões de leitura nas tabelas
2. **Performance**: Cada tabela executa 2 queries (DESCRIBE + SELECT)
3. **Rede**: Requer conexão com o cluster Impala
4. **Tamanho**: Os arquivos gerados podem ocupar espaço significativo

## ❓ Troubleshooting

### Erro: "SparkSession não encontrada"
```
❌ ERRO: SparkSession não encontrada!
```

**Solução**: Execute o script dentro de um notebook Jupyter/Zeppelin com Spark configurado.

### Erro: "Table not found"
```
❌ Erro ao processar tabela_xyz: Table not found
```

**Solução**: Verifique se a tabela existe no database `teste`:
```python
spark.sql("SHOW TABLES IN teste").show()
```

### Erro de permissões
```
❌ Access denied for user...
```

**Solução**: Verifique suas credenciais e permissões no cluster.

## 📞 Suporte

Para dúvidas ou problemas:
1. Verifique os logs gerados em `data-schemas/`
2. Consulte o arquivo `SUMMARY_REPORT.txt`
3. Arquivos de erro: `{tabela}_ERROR.txt`

## 🎯 Próximos Passos

Após gerar os schemas:

1. Revise os arquivos em `data-schemas/`
2. Valide a estrutura das tabelas
3. Use os schemas para documentar o projeto
4. Commit os arquivos no repositório (se necessário)

---

**Última atualização:** 2025-11-17
**Versão:** 1.0.0
