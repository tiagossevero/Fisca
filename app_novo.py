"""
FISCA - Sistema de Análise de Fiscalizações
Versão 2.0 - Refatorada e Otimizada

Sistema completo de Business Intelligence para análise de fiscalizações tributárias
Desenvolvido para SEFAZ-SC
"""

import streamlit as st
import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent))

from src.config.settings import PAGE_CONFIG, CUSTOM_CSS, SENHA_PADRAO
from src.modules.cache_manager import clear_all_caches, get_cache_stats

# Importar páginas
from src.pages import (
    dashboard_executivo,
    analise_temporal,
    analise_geografica,
    analise_performance,
    analise_cnae,
    machine_learning
)


# ===== CONFIGURAÇÃO DA PÁGINA =====
st.set_page_config(**PAGE_CONFIG)

# CSS Customizado
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ===== FUNÇÕES AUXILIARES =====

def check_password():
    """Sistema de autenticação"""

    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    st.title("🔐 FISCA - Login")
    st.markdown("### Sistema de Análise de Fiscalizações")

    with st.form("login_form"):
        password = st.text_input("Senha", type="password")
        submit = st.form_submit_button("Entrar")

        if submit:
            if password == SENHA_PADRAO:
                st.session_state.authenticated = True
                st.success("✅ Login realizado com sucesso!")
                st.rerun()
            else:
                st.error("❌ Senha incorreta!")

    st.info("""
    **Bem-vindo ao FISCA 2.0**

    Sistema completo de análise de fiscalizações com:
    - 📊 Dashboard Executivo Avançado
    - 📈 Análise Temporal com Tendências
    - 🗺️ Análise Geográfica Interativa
    - 🏢 Análise de Performance (Gerências e AFREs)
    - 🏭 Análise Setorial (CNAE)
    - 🤖 Machine Learning para Priorização
    - 📥 Exportação de Dados e Relatórios

    ---
    *Desenvolvido com Streamlit + Plotly + Scikit-learn*
    """)

    return False


def render_sidebar():
    """Renderiza sidebar com navegação e informações"""

    with st.sidebar:
        st.image("https://via.placeholder.com/200x100/667eea/ffffff?text=FISCA+2.0", use_container_width=True)

        st.markdown("---")

        # Navegação
        st.header("📑 Navegação")

        pages = {
            "📊 Dashboard Executivo": dashboard_executivo,
            "📈 Análise Temporal": analise_temporal,
            "🗺️ Análise Geográfica": analise_geografica,
            "🏢 Performance": analise_performance,
            "🏭 Análise por CNAE": analise_cnae,
            "🤖 Machine Learning": machine_learning
        }

        if 'current_page' not in st.session_state:
            st.session_state.current_page = "📊 Dashboard Executivo"

        for page_name in pages.keys():
            if st.button(
                page_name,
                use_container_width=True,
                type="primary" if st.session_state.current_page == page_name else "secondary"
            ):
                st.session_state.current_page = page_name
                st.rerun()

        st.markdown("---")

        # Ferramentas
        st.header("🛠️ Ferramentas")

        if st.button("🔄 Limpar Cache", use_container_width=True):
            clear_all_caches()
            st.rerun()

        if st.button("📊 Estatísticas Cache", use_container_width=True):
            st.markdown(get_cache_stats())

        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()

        st.markdown("---")

        # Informações
        st.header("ℹ️ Sobre")

        st.markdown("""
        **FISCA 2.0**

        Sistema de Business Intelligence para análise de fiscalizações tributárias.

        **Versão:** 2.0.0
        **Data:** 2025

        **Tecnologias:**
        - Streamlit
        - Plotly
        - Scikit-learn
        - Pandas
        - Apache Impala

        ---
        *© 2025 SEFAZ-SC*
        """)


def render_home():
    """Renderiza página inicial"""
    st.title("🏠 Bem-vindo ao FISCA 2.0")
    st.markdown("### Sistema Avançado de Análise de Fiscalizações")

    st.markdown("""
    ## 🎯 Sobre o Sistema

    O **FISCA 2.0** é um sistema completo de Business Intelligence desenvolvido para otimizar
    a análise e gestão de fiscalizações tributárias. Com tecnologias de ponta em visualização
    de dados e Machine Learning, oferecemos insights acionáveis para tomada de decisão.

    ---

    ## 📊 Módulos Disponíveis

    """)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        ### 📈 Análises Descritivas

        **Dashboard Executivo**
        - KPIs principais em tempo real
        - Gauges de performance vs meta
        - Alertas e recomendações automáticas
        - Evolução temporal completa
        - Rankings de top performers

        **Análise Temporal**
        - Tendências e sazonalidade
        - Médias móveis
        - Análise de crescimento
        - Detecção de anomalias

        **Análise Geográfica**
        - Distribuição por município
        - Mapas de concentração
        - Rankings regionais
        - Análise de correlação espacial
        """)

    with col2:
        st.markdown("""
        ### 🔬 Análises Avançadas

        **Análise de Performance**
        - Benchmarking de gerências
        - Produtividade de AFREs
        - Faixas de performance
        - Identificação de gaps

        **Análise Setorial (CNAE)**
        - Hierarquia de setores
        - Concentração setorial
        - Performance por indústria
        - Drill-down completo

        **Machine Learning**
        - Múltiplos algoritmos (RF, GB, LR)
        - Predição de conversão
        - Priorização inteligente
        - Feature importance
        - ROC/AUC análysis
        """)

    st.markdown("---")

    st.markdown("""
    ## 🚀 Começando

    1. **Navegue** pelos módulos usando o menu lateral
    2. **Configure** filtros para análises específicas
    3. **Explore** visualizações interativas
    4. **Exporte** dados para análises offline
    5. **Compartilhe** insights com sua equipe

    ---

    ## 💡 Recursos Principais

    """)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.info("""
        **⚡ Performance**
        - Cache inteligente
        - Queries otimizadas
        - Carregamento rápido
        - Interface responsiva
        """)

    with col2:
        st.success("""
        **📊 Visualizações**
        - Gráficos interativos
        - Múltiplos formatos
        - Exportação facilitada
        - Temas customizáveis
        """)

    with col3:
        st.warning("""
        **🤖 IA & ML**
        - Predições precisas
        - Priorização automática
        - Insights acionáveis
        - Aprendizado contínuo
        """)

    st.markdown("---")

    st.markdown("""
    ## 📞 Suporte

    Para dúvidas ou sugestões:
    - 📧 Email: suporte@fisca.gov.br
    - 📱 Ramal: 1234
    - 💬 Chat: [Interno SEFAZ]

    ---

    **👉 Selecione um módulo no menu lateral para começar!**
    """)


# ===== FUNÇÃO PRINCIPAL =====

def main():
    """Função principal da aplicação"""

    # Verificar autenticação
    if not check_password():
        return

    # Renderizar sidebar
    render_sidebar()

    # Obter página atual
    current_page = st.session_state.get('current_page', None)

    # Renderizar página selecionada
    if current_page == "📊 Dashboard Executivo":
        dashboard_executivo.render()
    elif current_page == "📈 Análise Temporal":
        analise_temporal.render()
    elif current_page == "🗺️ Análise Geográfica":
        analise_geografica.render()
    elif current_page == "🏢 Performance":
        analise_performance.render()
    elif current_page == "🏭 Análise por CNAE":
        analise_cnae.render()
    elif current_page == "🤖 Machine Learning":
        machine_learning.render()
    else:
        # Página home por padrão
        render_home()

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; padding: 20px;">
        <p>FISCA 2.0 - Sistema de Análise de Fiscalizações | © 2025 SEFAZ-SC</p>
        <p><em>Desenvolvido com ❤️ usando Streamlit</em></p>
    </div>
    """, unsafe_allow_html=True)


# ===== EXECUTAR APLICAÇÃO =====

if __name__ == "__main__":
    main()
