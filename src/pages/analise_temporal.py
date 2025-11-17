"""
Análise Temporal Avançada
Tendências, sazonalidade, previsões e padrões temporais
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime

from src.modules.database import get_database_connection
from src.modules.cache_manager import load_temporal_evolution
from src.modules.charts import ChartBuilder
from src.utils.helpers import DataProcessor, Formatter, create_download_button


def render():
    """Renderiza Análise Temporal"""
    st.title("📈 Análise Temporal Avançada")
    st.markdown("### Tendências, Sazonalidade e Padrões ao Longo do Tempo")

    db = get_database_connection()
    chart_builder = ChartBuilder()
    processor = DataProcessor()

    # Filtros
    with st.sidebar:
        st.header("⚙️ Configurações")

        periodo = st.selectbox(
            "Granularidade",
            options=['Mensal', 'Trimestral', 'Anual'],
            index=0
        )

        media_movel = st.slider("Janela Média Móvel", 1, 12, 3)
        mostrar_tendencia = st.checkbox("Mostrar Linha de Tendência", value=True)
        mostrar_previsao = st.checkbox("Mostrar Previsão Simples", value=False)

    # Carregar dados
    period_map = {'Mensal': 'mes', 'Trimestral': 'mes', 'Anual': 'ano'}
    temporal_df = load_temporal_evolution(db, period_map[periodo])

    if temporal_df is None or temporal_df.empty:
        st.warning("⚠️ Dados não disponíveis")
        return

    # Processar dados
    temporal_df['taxa_conversao'] = (temporal_df['qtd_nfs'] / temporal_df['qtd_infracoes'] * 100).fillna(0)
    temporal_df['valor_medio'] = (temporal_df['valor_total'] / temporal_df['qtd_infracoes']).fillna(0)

    # Médias móveis
    temporal_df['qtd_infracoes_ma'] = processor.calculate_moving_average(temporal_df['qtd_infracoes'], media_movel)
    temporal_df['taxa_conversao_ma'] = processor.calculate_moving_average(temporal_df['taxa_conversao'], media_movel)

    # Crescimento
    temporal_df['crescimento_infracoes'] = processor.calculate_growth_rate(temporal_df['qtd_infracoes'])
    temporal_df['crescimento_nfs'] = processor.calculate_growth_rate(temporal_df['qtd_nfs'])

    # KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Períodos",
            len(temporal_df),
            delta=None
        )

    with col2:
        crescimento_medio = temporal_df['crescimento_infracoes'].mean()
        st.metric(
            "Crescimento Médio",
            f"{crescimento_medio:.1f}%",
            delta=f"{'↑' if crescimento_medio > 0 else '↓'}"
        )

    with col3:
        tendencia_recente = temporal_df['qtd_infracoes'].tail(3).mean() - temporal_df['qtd_infracoes'].head(3).mean()
        st.metric(
            "Tendência Recente",
            "Crescente" if tendencia_recente > 0 else "Decrescente",
            delta=f"{tendencia_recente:.0f}"
        )

    with col4:
        volatilidade = temporal_df['qtd_infracoes'].std()
        st.metric(
            "Volatilidade",
            f"{volatilidade:.0f}",
            delta="Desvio Padrão"
        )

    st.markdown("---")

    # Gráfico principal: Volume com média móvel
    st.markdown("### 📊 Evolução de Volume")

    fig = chart_builder.create_time_series(
        df=temporal_df,
        x_col='periodo',
        y_cols=['qtd_infracoes', 'qtd_infracoes_ma'],
        title="Volume de Infrações com Média Móvel",
        labels={'qtd_infracoes': 'Infrações', 'qtd_infracoes_ma': f'Média Móvel ({media_movel} períodos)'},
        show_trend=mostrar_tendencia
    )
    st.plotly_chart(fig, use_container_width=True)

    # Taxa de conversão temporal
    st.markdown("### 🎯 Evolução da Taxa de Conversão")

    fig_conv = chart_builder.create_time_series(
        df=temporal_df,
        x_col='periodo',
        y_cols=['taxa_conversao', 'taxa_conversao_ma'],
        title="Taxa de Conversão ao Longo do Tempo",
        labels={'taxa_conversao': 'Taxa Conversão (%)', 'taxa_conversao_ma': f'Média Móvel ({media_movel})'},
        show_trend=mostrar_tendencia
    )
    st.plotly_chart(fig_conv, use_container_width=True)

    # Análise de crescimento
    st.markdown("### 📈 Análise de Crescimento Período a Período")

    col1, col2 = st.columns(2)

    with col1:
        fig_cresc = chart_builder.create_bar_chart(
            df=temporal_df.tail(12),
            x_col='periodo',
            y_col='crescimento_infracoes',
            title="Crescimento de Infrações (%)",
            orientation='v'
        )
        st.plotly_chart(fig_cresc, use_container_width=True)

    with col2:
        fig_cresc_nf = chart_builder.create_bar_chart(
            df=temporal_df.tail(12),
            x_col='periodo',
            y_col='crescimento_nfs',
            title="Crescimento de Notificações (%)",
            orientation='v'
        )
        st.plotly_chart(fig_cresc_nf, use_container_width=True)

    # Comparativo valores
    st.markdown("### 💰 Evolução de Valores")

    fig_valores = chart_builder.create_area_chart(
        df=temporal_df,
        x_col='periodo',
        y_cols=['valor_total'],
        title="Valores Totais Autuados",
        stacked=False
    )
    st.plotly_chart(fig_valores, use_container_width=True)

    # Tabela de dados
    st.markdown("### 📋 Dados Detalhados")

    display_df = temporal_df[['periodo', 'qtd_infracoes', 'qtd_nfs', 'taxa_conversao', 'valor_total']].copy()
    display_df.columns = ['Período', 'Infrações', 'Notificações', 'Taxa Conv. (%)', 'Valor Total']

    st.dataframe(
        display_df.style.format({
            'Infrações': '{:,.0f}',
            'Notificações': '{:,.0f}',
            'Taxa Conv. (%)': '{:.2f}%',
            'Valor Total': 'R$ {:,.2f}'
        }).background_gradient(subset=['Taxa Conv. (%)'], cmap='RdYlGn'),
        use_container_width=True
    )

    # Exportar
    create_download_button(temporal_df, f"analise_temporal_{datetime.now().strftime('%Y%m%d')}.csv")
