"""
Análise por CNAE
Análise setorial com hierarquia e drill-down
"""

import streamlit as st
from datetime import datetime

from src.modules.database import get_database_connection
from src.modules.cache_manager import load_performance_by_cnae
from src.modules.charts import ChartBuilder
from src.utils.helpers import Calculator, Formatter, create_download_button


def render():
    """Renderiza Análise por CNAE"""
    st.title("🏭 Análise por CNAE")
    st.markdown("### Análise Setorial de Fiscalizações")

    db = get_database_connection()
    chart_builder = ChartBuilder()
    calc = Calculator()

    # Carregar dados
    cnae_df = load_performance_by_cnae(db)

    if cnae_df is None or cnae_df.empty:
        st.warning("⚠️ Dados não disponíveis")
        return

    # Filtros
    with st.sidebar:
        st.header("⚙️ Filtros")

        nivel = st.radio("Nível de Análise", ['Seção', 'Divisão'], index=0)
        top_n = st.slider("Top N Setores", 10, 30, 15)

    # Agregar por seção ou divisão
    if nivel == 'Seção':
        agg_df = cnae_df.groupby('cnae_secao').agg({
            'qtd_infracoes': 'sum',
            'qtd_nfs': 'sum',
            'valor_total': 'sum',
            'qtd_empresas': 'sum'
        }).reset_index()
        agg_df.columns = ['Setor', 'Infrações', 'NFs', 'Valor Total', 'Empresas']
    else:
        agg_df = cnae_df.groupby(['cnae_secao', 'cnae_divisao']).agg({
            'qtd_infracoes': 'sum',
            'qtd_nfs': 'sum',
            'valor_total': 'sum',
            'qtd_empresas': 'sum'
        }).reset_index()
        agg_df.columns = ['Seção', 'Divisão', 'Infrações', 'NFs', 'Valor Total', 'Empresas']
        agg_df['Setor'] = agg_df['Seção'] + ' - ' + agg_df['Divisão']

    agg_df['Taxa Conv.'] = (agg_df['NFs'] / agg_df['Infrações'] * 100).fillna(0)

    # KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Setores Únicos", len(agg_df))

    with col2:
        concentracao = calc.calculate_concentration_index(agg_df, 'Valor Total', 5)
        st.metric("Concentração Top 5", f"{concentracao:.1f}%")

    with col3:
        st.metric("Valor Total", Formatter.abbreviate_number(agg_df['Valor Total'].sum()))

    with col4:
        taxa_media = agg_df['Taxa Conv.'].mean()
        st.metric("Taxa Conv. Média", f"{taxa_media:.1f}%")

    st.markdown("---")

    # Rankings
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"### 🏆 Top {top_n} por Valor")

        top_valor = agg_df.nlargest(top_n, 'Valor Total')

        fig = chart_builder.create_horizontal_bar_ranking(
            df=top_valor,
            category_col='Setor',
            value_col='Valor Total',
            title="",
            top_n=top_n,
            color_scale=True
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown(f"### 📊 Top {top_n} por Volume")

        top_qtd = agg_df.nlargest(top_n, 'Infrações')

        fig_qtd = chart_builder.create_horizontal_bar_ranking(
            df=top_qtd,
            category_col='Setor',
            value_col='Infrações',
            title="",
            top_n=top_n,
            color_scale=True
        )
        st.plotly_chart(fig_qtd, use_container_width=True)

    # Distribuição
    st.markdown("### 📊 Distribuição de Valor")

    top_10 = agg_df.nlargest(10, 'Valor Total')

    fig_pizza = chart_builder.create_pie_chart(
        df=top_10,
        names_col='Setor',
        values_col='Valor Total',
        title=f"Top 10 Setores - Distribuição de Valor",
        hole=0.4
    )
    st.plotly_chart(fig_pizza, use_container_width=True)

    # Scatter
    st.markdown("### 🔍 Análise de Performance")

    fig_scatter = chart_builder.create_scatter_plot(
        df=agg_df.nlargest(30, 'Valor Total'),
        x_col='Infrações',
        y_col='Valor Total',
        title="Volume vs Valor por Setor",
        size_col='Empresas',
        color_col='Taxa Conv.',
        trendline=True
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    # Tabela
    st.markdown("### 📋 Dados Detalhados")

    st.dataframe(
        agg_df.style.format({
            'Infrações': '{:,.0f}',
            'NFs': '{:,.0f}',
            'Valor Total': 'R$ {:,.2f}',
            'Empresas': '{:,.0f}',
            'Taxa Conv.': '{:.2f}%'
        }).background_gradient(subset=['Taxa Conv.'], cmap='RdYlGn'),
        use_container_width=True,
        height=400
    )

    create_download_button(agg_df, f"analise_cnae_{datetime.now().strftime('%Y%m%d')}.csv")
