# 🎯 Sistema FISCA - Análise de Fiscalizações

Sistema de Análise e Monitoramento de Fiscalizações da Receita Estadual de Santa Catarina, desenvolvido para otimizar o acompanhamento, análise e gestão inteligente das ações fiscais de ICMS.

![Versão](https://img.shields.io/badge/versão-1.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.28+-red.svg)
![Licença](https://img.shields.io/badge/licença-SEFAZ--SC-green.svg)

## 📋 Índice

- [Sobre o Projeto](#-sobre-o-projeto)
- [Funcionalidades Principais](#-funcionalidades-principais)
- [Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Configuração](#-configuração)
- [Como Usar](#-como-usar)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Módulos e Páginas](#-módulos-e-páginas)
- [Machine Learning](#-machine-learning)
- [Métricas e KPIs](#-métricas-e-kpis)
- [Segurança](#-segurança)
- [Contribuindo](#-contribuindo)
- [Suporte](#-suporte)
- [Licença](#-licença)

## 🎯 Sobre o Projeto

O **Sistema FISCA** é uma plataforma completa de Business Intelligence desenvolvida para a Secretaria da Fazenda do Estado de Santa Catarina (SEFAZ-SC), focada no monitoramento e análise de fiscalizações tributárias.

### Objetivos Principais

1. **Aumentar a Taxa de Conversão**: Meta de ≥70% de conversão de Infrações em Notificações Fiscais
2. **Reduzir Tempo Médio**: Otimizar o tempo médio de fiscalização (meta: ≤60 dias)
3. **Priorizar Ações de Alto Impacto**: Utilizar Machine Learning para identificar fiscalizações prioritárias
4. **Melhorar Produtividade**: Benchmarking e identificação de melhores práticas entre AFREs e gerências

### Período de Análise

- **Histórico**: 2020 até a data atual
- **Atualização**: Dados atualizados periodicamente via integração com Impala/Hadoop

## ✨ Funcionalidades Principais

### 1. 📊 Dashboard Executivo
- Visão consolidada do sistema com KPIs em tempo real
- Evolução temporal de infrações e notificações fiscais
- Análise de produtividade dos Auditores Fiscais (AFREs)
- Métricas de conversão e tempestividade

### 2. 🏢 Análise por Gerência (GRAF)
- Performance detalhada por gerência regional
- Ranking de gerências por múltiplos critérios
- Taxa de conversão e análise de efetividade
- Valores lançados e distribuição geográfica

### 3. 🏭 Análise por Setor Econômico (CNAE)
- Distribuição de fiscalizações por setor
- Identificação de setores críticos
- Análise de volume e valores por CNAE
- Comparativos entre seções e divisões

### 4. 🗺️ Análise Geográfica
- Concentração de fiscalizações por município
- Ranking de municípios fiscalizados
- Análise de distribuição territorial
- Identificação de hotspots

### 5. 👥 Performance de AFREs
- Produtividade individual de auditores
- Ranking de AFREs por múltiplos indicadores
- Taxa de conversão por auditor
- Distribuição de faixas de produtividade
- Análise de meses ativos e volume de trabalho

### 6. ⚖️ Tipos de Infrações
- Catálogo completo de infrações
- Infrações mais comuns e recorrentes
- Análise de valores por tipo de infração
- Tendências temporais
- Top infrações por volume e valor

### 7. 🔎 Drill-Down por Empresa
- Busca avançada por CNPJ ou razão social
- Histórico completo de fiscalizações
- Detalhamento de cada infração
- Identificação de AFREs envolvidos
- Análise temporal e evolutiva

### 8. 🤖 Machine Learning
- Modelo preditivo de conversão (Infração → NF)
- Priorização inteligente de fiscalizações
- Feature importance e análise de drivers
- Recomendações automáticas de empresas prioritárias
- Métricas de performance do modelo (Acurácia, Precisão, Recall, F1-Score)
- Curva ROC e matriz de confusão

## 🛠️ Tecnologias Utilizadas

### Backend & Data Processing
- **Python 3.8+**: Linguagem principal
- **Pandas**: Manipulação e análise de dados
- **NumPy**: Computação científica
- **SQLAlchemy**: ORM e conexão com bancos de dados

### Frontend & Visualização
- **Streamlit**: Framework para aplicações web interativas
- **Plotly**: Gráficos interativos e visualizações avançadas
- **Plotly Express**: Visualizações simplificadas

### Machine Learning
- **Scikit-learn**: Algoritmos de ML
  - Random Forest Classifier
  - Gradient Boosting Classifier
  - StandardScaler
- **Métricas**: Classification Report, Confusion Matrix, ROC AUC

### Banco de Dados
- **Apache Impala**: Queries de alta performance em Hadoop
- **Hadoop/HDFS**: Armazenamento distribuído

### Segurança
- **Hashlib**: Criptografia e hashing
- **SSL**: Conexões seguras
- **Autenticação**: Sistema de senha customizado

## 📦 Pré-requisitos

### Requisitos de Sistema
- Python 3.8 ou superior
- Acesso à rede interna da SEFAZ-SC
- Credenciais válidas para o banco Impala

### Dependências Python

```bash
streamlit>=1.28.0
pandas>=1.5.0
numpy>=1.24.0
plotly>=5.17.0
sqlalchemy>=2.0.0
scikit-learn>=1.3.0
impyla>=0.18.0
```

## 🚀 Instalação

### 1. Clone o Repositório

```bash
git clone https://github.com/sefaz-sc/fisca.git
cd fisca
```

### 2. Crie um Ambiente Virtual

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Instale as Dependências

```bash
pip install -r requirements.txt
```

### 4. Configure as Credenciais

Crie um arquivo `.streamlit/secrets.toml`:

```toml
[impala_credentials]
user = "seu_usuario"
password = "sua_senha"
```

## ⚙️ Configuração

### Configuração de Conexão

Edite as variáveis de conexão no arquivo `FISCA.py`:

```python
IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'teste'
```

### Configuração de Senha

Por padrão, a senha do sistema é `fisca2025`. Para alterá-la, edite a linha 44 do arquivo `FISCA.py`:

```python
SENHA = "sua_senha_personalizada"
```

### Configuração de Pools de Recursos

Para otimizar queries, configure o pool adequado:

```sql
SET REQUEST_POOL = 'medium';
```

## 💻 Como Usar

### Executar a Aplicação

```bash
streamlit run FISCA.py
```

A aplicação estará disponível em: `http://localhost:8501`

### Login

1. Acesse a aplicação
2. Digite a senha configurada (padrão: `fisca2025`)
3. Clique em "Entrar"

### Navegação

Use o menu lateral para navegar entre as páginas:
- **Dashboard Executivo**: Visão geral do sistema
- **Análise por Gerência**: Performance regional
- **Análise por CNAE**: Setores econômicos
- **Análise por Município**: Distribuição geográfica
- **Performance AFREs**: Produtividade individual
- **Tipos de Infrações**: Catálogo e estatísticas
- **Drill-Down Empresa**: Busca detalhada
- **Machine Learning**: Priorização inteligente
- **Sobre o Sistema**: Informações e documentação

### Filtros Globais

Na sidebar, você pode aplicar filtros:
- **Anos**: Selecione os anos para análise
- **Gerências**: Filtre por gerências específicas
- **Valor Mínimo**: Defina o valor mínimo de infrações
- **Tema dos Gráficos**: Escolha o tema visual

## 📁 Estrutura do Projeto

```
Fisca/
├── FISCA.py                    # Aplicação principal Streamlit
├── FISCA-Copy1.py             # Versão alternativa/backup
├── FISCA.ipynb                # Notebook Jupyter principal
├── FISCA-Exemplo (1).ipynb    # Notebook de exemplos
├── FISCA.json                 # Metadados e configurações
├── README.md                  # Este arquivo
├── requirements.txt           # Dependências Python (a criar)
├── .streamlit/
│   └── secrets.toml          # Credenciais (não versionado)
└── .gitignore                # Arquivos ignorados pelo Git
```

## 📊 Módulos e Páginas

### Dashboard Executivo (`pagina_dashboard_executivo`)
- **KPIs Principais**: Total de infrações, empresas, NFs emitidas, valores
- **Evolução Temporal**: Gráficos de tendência por ano
- **Taxa de Conversão**: Análise de efetividade
- **Produtividade**: Infrações por AFRE

### Análise por Gerência (`pagina_analise_gerencias`)
- **Performance Consolidada**: Métricas agregadas por gerência
- **Top 10 Gerências**: Ranking por valor lançado
- **Taxa de Conversão vs Volume**: Análise scatter
- **Tabela Detalhada**: Ranking completo com todas as métricas

### Análise por CNAE (`pagina_analise_cnae`)
- **Seleção de Nível**: Seção (macro) ou Divisão (detalhado)
- **Top 15 Setores**: Por valor lançado
- **Distribuição**: Pizza dos principais setores
- **Tabela Completa**: Todos os CNAEs com métricas

### Análise Geográfica (`pagina_analise_municipios`)
- **Concentração**: Métricas de concentração geográfica
- **Top 20 Municípios**: Por volume e por valor
- **Ranking Completo**: Todos os municípios fiscalizados

### Performance AFREs (`pagina_analise_afres`)
- **Estatísticas Gerais**: Média de NFs/mês, taxa de conversão
- **Distribuição de Produtividade**: Faixas e histograma
- **Ranking de AFREs**: Top performers configurável

### Tipos de Infrações (`pagina_tipos_infracoes`)
- **Estatísticas**: Total de tipos, ocorrências, valores
- **Top 30 Infrações**: Por volume e por valor
- **Tabela Completa**: Catálogo com todas as infrações

### Drill-Down Empresa (`pagina_drill_down_empresa`)
- **Busca**: Por CNPJ ou razão social
- **Dados Cadastrais**: Informações da empresa
- **Histórico**: Todas as fiscalizações
- **Detalhes**: Drill-down em fiscalizações específicas
- **AFREs Envolvidos**: Auditores responsáveis

## 🤖 Machine Learning

### Modelo Preditivo

O sistema utiliza algoritmos de Machine Learning para priorizar fiscalizações:

#### Algoritmos Disponíveis
- **Random Forest Classifier**: Ensemble de árvores de decisão
- **Gradient Boosting Classifier**: Boosting sequencial

#### Features Utilizadas
- **Numéricas**:
  - Log do valor da infração
  - Dias até emissão da NF
  - Ano da infração

- **Categóricas** (One-Hot Encoding):
  - Regime tributário (Simples Nacional, Regime Normal)
  - Top 5 CNAEs
  - Tipo de infração

#### Target
- **Gerou Notificação**: Binário (0/1)

#### Métricas de Avaliação
- **Acurácia**: Percentual de previsões corretas
- **Precisão**: Percentual de verdadeiros positivos
- **Recall**: Capacidade de identificar casos positivos
- **F1-Score**: Média harmônica entre precisão e recall
- **AUC-ROC**: Área sob a curva ROC

#### Priorização
Score de prioridade calculado como:
```python
score_prioridade = (probabilidade_nf * 0.6) + (valor_normalizado * 0.4)
```

### Como Usar o ML

1. Acesse a página "Machine Learning"
2. Configure:
   - **Algoritmo**: Random Forest ou Gradient Boosting
   - **% Teste**: Percentual do dataset para teste (10-40%)
   - **Threshold**: Limite de classificação (0.3-0.7)
3. Clique em "Treinar Modelo e Gerar Recomendações"
4. Analise:
   - Métricas de performance
   - Matriz de confusão
   - Curva ROC
   - Feature importance
   - Lista de empresas prioritárias
5. Faça download da lista de recomendações (CSV)

## 📈 Métricas e KPIs

### KPIs Principais

| Métrica | Descrição | Meta |
|---------|-----------|------|
| **Taxa de Conversão** | (NFs Emitidas / Infrações) × 100 | ≥ 70% |
| **Dias Médios (Infração → NF)** | Tempo médio para emissão de NF | ≤ 60 dias |
| **Ciclos Completos** | Infrações com NF e Encerramento | Maximizar |
| **Valor Médio por Infração** | Valor total / Quantidade | Monitorar |
| **Produtividade AFRE** | NFs por mês por auditor | Benchmarking |

### Indicadores de Qualidade

- **Taxa de Ciência**: Percentual de infrações com ciência
- **Taxa de Julgamento**: Percentual de infrações julgadas
- **Taxa de Regularização**: Empresas regularizadas sem NF
- **Taxa de Cancelamento**: Infrações canceladas

## 🔒 Segurança

### Autenticação
- Sistema de senha customizado
- Sessões protegidas com Streamlit Session State

### Conexões
- SSL/TLS para conexões Impala
- Autenticação LDAP
- Credenciais armazenadas em `secrets.toml` (não versionado)

### Boas Práticas
- Nunca commite o arquivo `secrets.toml`
- Use senhas fortes e únicas
- Mantenha as credenciais atualizadas
- Revise logs de acesso regularmente

## 🤝 Contribuindo

### Como Contribuir

1. **Fork** o projeto
2. Crie uma **branch** para sua feature (`git checkout -b feature/NovaFuncionalidade`)
3. **Commit** suas mudanças (`git commit -m 'Add: Nova funcionalidade X'`)
4. **Push** para a branch (`git push origin feature/NovaFuncionalidade`)
5. Abra um **Pull Request**

### Padrões de Código

- Siga o **PEP 8** para Python
- Documente funções complexas
- Adicione comentários explicativos
- Mantenha a consistência visual
- Teste antes de commitar

### Reportar Bugs

Abra uma issue incluindo:
- Descrição detalhada do problema
- Passos para reproduzir
- Comportamento esperado vs observado
- Screenshots (se aplicável)
- Versão do Python e dependências

## 📞 Suporte

### Contatos

- **Desenvolvedor**: Thiago Severo
- **Equipe**: TI - SEFAZ/SC
- **Email**: [suporte@sefaz.sc.gov.br](mailto:suporte@sefaz.sc.gov.br)

### Documentação Adicional

- [Manual do Usuário](docs/manual_usuario.pdf)
- [Documentação Técnica](docs/documentacao_tecnica.pdf)
- [FAQ](docs/faq.md)

### Canais de Suporte

1. **Email**: Para questões técnicas e bugs
2. **Teams**: Para dúvidas rápidas
3. **Issues GitHub**: Para bugs e melhorias
4. **Wiki**: Para documentação colaborativa

## 📄 Licença

Este projeto é de propriedade da **Secretaria da Fazenda do Estado de Santa Catarina (SEFAZ-SC)** e destina-se exclusivamente ao uso interno.

**Direitos Reservados © 2025 SEFAZ-SC**

---

## 🏆 Changelog

### Versão 1.0 (Outubro 2025)
- ✅ Lançamento inicial do Sistema FISCA
- ✅ Dashboard Executivo completo
- ✅ 8 módulos de análise
- ✅ Sistema de Machine Learning
- ✅ Integração com Impala/Hadoop
- ✅ Interface responsiva e intuitiva
- ✅ Sistema de autenticação
- ✅ Filtros dinâmicos
- ✅ Exportação de dados

---

## 🙏 Agradecimentos

- **SEFAZ-SC**: Pelo suporte institucional
- **Equipe de AFREs**: Pelos feedbacks e requisitos
- **TI SEFAZ**: Pela infraestrutura e suporte técnico
- **Comunidade Python**: Pelas bibliotecas open-source

---

<div align="center">

**Sistema FISCA v1.0** | Desenvolvido com ❤️ para a SEFAZ-SC

*Otimizando a fiscalização tributária através de dados e inteligência artificial*

</div>
