"""
Análise Geográfica
Distribuição espacial, mapas e concentração por município
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from src.modules.database import get_database_connection
from src.modules.cache_manager import load_geographic_analysis
from src.modules.charts import ChartBuilder
from src.utils.helpers import Calculator, Formatter, create_download_button


def render():
    """Renderiza Análise Geográfica"""
    st.title("🗺️ Análise Geográfica")
    st.markdown("### Distribuição Espacial de Fiscalizações por Município")

    db = get_database_connection()
    chart_builder = ChartBuilder()
    calc = Calculator()

    # Filtros
    with st.sidebar:
        st.header("⚙️ Filtros")

        top_n = st.slider("Quantidade de Municípios", 10, 50, 20)

        metrica_ordem = st.selectbox(
            "Ordenar por",
            options=['Valor Total', 'Quantidade', 'Taxa de Conversão'],
            index=0
        )

    # Carregar dados
    geo_df = load_geographic_analysis(db)

    if geo_df is None or geo_df.empty:
        st.warning("⚠️ Dados geográficos não disponíveis")
        return

    # Mapear métrica
    metric_map = {
        'Valor Total': 'valor_total',
        'Quantidade': 'qtd_infracoes',
        'Taxa de Conversão': 'taxa_conversao'
    }
    sort_col = metric_map[metrica_ordem]

    # KPIs Gerais
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total de Municípios", len(geo_df))

    with col2:
        concentracao_top5 = calc.calculate_concentration_index(geo_df, 'valor_total', 5)
        st.metric("Concentração Top 5", f"{concentracao_top5:.1f}%")

    with col3:
        concentracao_top10 = calc.calculate_concentration_index(geo_df, 'valor_total', 10)
        st.metric("Concentração Top 10", f"{concentracao_top10:.1f}%")

    with col4:
        valor_total = geo_df['valor_total'].sum()
        st.metric("Valor Total", Formatter.abbreviate_number(valor_total))

    st.markdown("---")

    # Rankings
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"### 🏆 Top {top_n} por Valor")

        top_valor = geo_df.nlargest(top_n, 'valor_total')

        fig_valor = chart_builder.create_horizontal_bar_ranking(
            df=top_valor,
            category_col='municipio',
            value_col='valor_total',
            title="",
            top_n=top_n,
            color_scale=True
        )
        st.plotly_chart(fig_valor, use_container_width=True)

    with col2:
        st.markdown(f"### 📊 Top {top_n} por Quantidade")

        top_qtd = geo_df.nlargest(top_n, 'qtd_infracoes')

        fig_qtd = chart_builder.create_horizontal_bar_ranking(
            df=top_qtd,
            category_col='municipio',
            value_col='qtd_infracoes',
            title="",
            top_n=top_n,
            color_scale=True
        )
        st.plotly_chart(fig_qtd, use_container_width=True)

    # Distribuição por UF (agregado)
    st.markdown("### 🏳️ Distribuição por Estado")

    uf_agg = geo_df.groupby('uf').agg({
        'qtd_infracoes': 'sum',
        'qtd_nfs': 'sum',
        'valor_total': 'sum',
        'municipio': 'count'
    }).reset_index()

    uf_agg.columns = ['UF', 'Infrações', 'Notificações', 'Valor Total', 'Municípios']
    uf_agg['Taxa Conversão'] = (uf_agg['Notificações'] / uf_agg['Infrações'] * 100).fillna(0)

    col1, col2 = st.columns(2)

    with col1:
        fig_uf_valor = chart_builder.create_pie_chart(
            df=uf_agg,
            names_col='UF',
            values_col='Valor Total',
            title="Distribuição de Valor por Estado",
            hole=0.4
        )
        st.plotly_chart(fig_uf_valor, use_container_width=True)

    with col2:
        fig_uf_qtd = chart_builder.create_bar_chart(
            df=uf_agg,
            x_col='UF',
            y_col='Infrações',
            title="Infrações por Estado",
            orientation='v'
        )
        st.plotly_chart(fig_uf_qtd, use_container_width=True)

    # Scatter: Volume vs Valor
    st.markdown("### 📊 Análise de Correlação: Volume vs Valor")

    fig_scatter = chart_builder.create_scatter_plot(
        df=geo_df.nlargest(50, 'valor_total'),
        x_col='qtd_infracoes',
        y_col='valor_total',
        title="Relação entre Volume e Valor por Município",
        size_col='qtd_empresas',
        color_col='taxa_conversao',
        trendline=True
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    # Tabela completa
    st.markdown("### 📋 Dados Completos")

    display_df = geo_df[['municipio', 'uf', 'qtd_infracoes', 'qtd_nfs', 'taxa_conversao', 'valor_total', 'qtd_empresas']].copy()
    display_df.columns = ['Município', 'UF', 'Infrações', 'NFs', 'Taxa Conv. (%)', 'Valor Total', 'Empresas']

    st.dataframe(
        display_df.style.format({
            'Infrações': '{:,.0f}',
            'NFs': '{:,.0f}',
            'Taxa Conv. (%)': '{:.2f}%',
            'Valor Total': 'R$ {:,.2f}',
            'Empresas': '{:,.0f}'
        }).background_gradient(subset=['Valor Total'], cmap='Blues'),
        use_container_width=True,
        height=400
    )

    # Exportar
    create_download_button(geo_df, f"analise_geografica_{datetime.now().strftime('%Y%m%d')}.csv")
