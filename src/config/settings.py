"""
Configurações Centralizadas do Sistema FISCA
"""

# Configurações de Banco de Dados
IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'teste'
RESOURCE_POOL = 'medium'

# Configurações de Autenticação
SENHA_PADRAO = "fisca2025"

# Configurações de Cache
CACHE_TTL_GLOBAL = 3600  # 1 hora
CACHE_TTL_CONSULTAS = 1800  # 30 minutos
CACHE_TTL_ML = 7200  # 2 horas

# Configurações de Visualização
THEME_COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e',
    'success': '#2ca02c',
    'warning': '#ff9800',
    'danger': '#d62728',
    'info': '#17a2b8',
    'light': '#f8f9fa',
    'dark': '#343a40'
}

COLOR_SCALES = {
    'sequential_blue': 'Blues',
    'sequential_green': 'Greens',
    'sequential_red': 'Reds',
    'diverging': 'RdYlGn',
    'categorical': 'Set3',
    'heatmap': 'Viridis'
}

# Configurações de KPIs
META_CONVERSAO = 70.0  # Meta de conversão em %
META_DIAS_NOTIFICACAO = 60  # Meta de dias para notificação
META_PRODUTIVIDADE_AFRE = 5.0  # Meta de NFs por mês

# Configurações de ML
ML_TEST_SIZE_DEFAULT = 0.2
ML_RANDOM_STATE = 42
ML_MODELS = {
    'random_forest': {
        'name': 'Random Forest',
        'n_estimators': 100,
        'max_depth': 10,
        'min_samples_split': 5
    },
    'gradient_boosting': {
        'name': 'Gradient Boosting',
        'n_estimators': 100,
        'learning_rate': 0.1,
        'max_depth': 5
    },
    'logistic_regression': {
        'name': 'Logistic Regression',
        'max_iter': 1000,
        'C': 1.0
    }
}

# Configurações de Filtros
ANOS_DISPONIVEIS = list(range(2020, 2026))
LIMITE_REGISTROS_TABELA = 1000

# Configurações de Exportação
EXPORT_FORMATS = ['CSV', 'Excel', 'JSON']
MAX_EXPORT_ROWS = 100000

# Mensagens do Sistema
MENSAGENS = {
    'carregando': '⏳ Carregando dados...',
    'erro_conexao': '❌ Erro na conexão com o banco de dados',
    'sem_dados': '⚠️ Nenhum dado encontrado para os filtros selecionados',
    'processando': '⚙️ Processando...',
    'sucesso': '✅ Operação concluída com sucesso',
    'exportando': '📥 Exportando dados...'
}

# Configurações de Página
PAGE_CONFIG = {
    'page_title': 'FISCA - Sistema de Análise de Fiscalizações',
    'page_icon': '📊',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded'
}

# CSS Customizado
CUSTOM_CSS = """
<style>
    /* Estilo Global */
    .main {
        padding: 0rem 1rem;
    }

    /* Cards de Métricas */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin: 0.5rem 0;
    }

    .metric-card-success {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }

    .metric-card-warning {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    }

    .metric-card-info {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }

    /* Tabelas */
    .styled-table {
        border-collapse: collapse;
        margin: 25px 0;
        font-size: 0.9em;
        min-width: 400px;
        box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
        border-radius: 8px;
        overflow: hidden;
    }

    .styled-table thead tr {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: #ffffff;
        text-align: left;
        font-weight: bold;
    }

    .styled-table th,
    .styled-table td {
        padding: 12px 15px;
    }

    .styled-table tbody tr {
        border-bottom: 1px solid #dddddd;
        transition: background-color 0.3s;
    }

    .styled-table tbody tr:nth-of-type(even) {
        background-color: #f3f3f3;
    }

    .styled-table tbody tr:hover {
        background-color: #f1f1f1;
        cursor: pointer;
    }

    .styled-table tbody tr:last-of-type {
        border-bottom: 2px solid #667eea;
    }

    /* Alertas */
    .alert-success {
        background-color: #d4edda;
        border-color: #c3e6cb;
        color: #155724;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }

    .alert-warning {
        background-color: #fff3cd;
        border-color: #ffeaa7;
        color: #856404;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }

    .alert-danger {
        background-color: #f8d7da;
        border-color: #f5c6cb;
        color: #721c24;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }

    .alert-info {
        background-color: #d1ecf1;
        border-color: #bee5eb;
        color: #0c5460;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }

    /* Indicadores */
    .indicator {
        display: inline-block;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        margin-right: 8px;
    }

    .indicator-success { background-color: #28a745; }
    .indicator-warning { background-color: #ffc107; }
    .indicator-danger { background-color: #dc3545; }
    .indicator-info { background-color: #17a2b8; }

    /* Sidebar */
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }

    /* Títulos */
    h1, h2, h3 {
        color: #667eea;
    }

    /* Botões */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 5px;
        padding: 0.5rem 1rem;
        transition: all 0.3s;
    }

    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    }

    /* Progress Bar */
    .stProgress > div > div > div > div {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
</style>
"""
