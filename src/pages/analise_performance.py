"""
Análise de Performance
Benchmarking de Gerências e AFREs
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from src.modules.database import get_database_connection
from src.modules.cache_manager import load_performance_by_gerencia, load_afre_performance
from src.modules.charts import ChartBuilder
from src.utils.helpers import AlertGenerator, create_download_button
from src.config.settings import META_CONVERSAO, META_PRODUTIVIDADE_AFRE


def render():
    """Renderiza Análise de Performance"""
    st.title("📊 Análise de Performance")
    st.markdown("### Benchmarking de Gerências e AFREs")

    db = get_database_connection()
    chart_builder = ChartBuilder()
    alert_gen = AlertGenerator()

    # Tabs
    tab1, tab2 = st.tabs(["🏢 Gerências", "👥 AFREs"])

    # ===== TAB GERÊNCIAS =====
    with tab1:
        st.markdown("## Análise por Gerência")

        gerencia_df = load_performance_by_gerencia(db)

        if gerencia_df is None or gerencia_df.empty:
            st.warning("⚠️ Dados não disponíveis")
            return

        # KPIs
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Gerências", len(gerencia_df))

        with col2:
            taxa_media = gerencia_df['taxa_conversao'].mean()
            st.metric("Taxa Conv. Média", f"{taxa_media:.1f}%")

        with col3:
            melhor_gerencia = gerencia_df.nlargest(1, 'taxa_conversao').iloc[0]
            st.metric("Melhor Taxa", f"{melhor_gerencia['taxa_conversao']:.1f}%",
                     delta=melhor_gerencia['gerencia'])

        with col4:
            total_valor = gerencia_df['valor_total'].sum()
            st.metric("Valor Total", f"R$ {total_valor/1e6:.1f}M")

        st.markdown("---")

        # Rankings
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 🏆 Top 10 por Valor")

            top_valor = gerencia_df.nlargest(10, 'valor_total')

            fig = chart_builder.create_horizontal_bar_ranking(
                df=top_valor,
                category_col='gerencia',
                value_col='valor_total',
                title="",
                top_n=10,
                color_scale=True
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### 📈 Top 10 por Taxa de Conversão")

            top_conv = gerencia_df.nlargest(10, 'taxa_conversao')

            fig_conv = chart_builder.create_horizontal_bar_ranking(
                df=top_conv,
                category_col='gerencia',
                value_col='taxa_conversao',
                title="",
                top_n=10,
                color_scale=True
            )
            st.plotly_chart(fig_conv, use_container_width=True)

        # Scatter
        st.markdown("### 🔍 Análise Volume vs Taxa de Conversão")

        fig_scatter = chart_builder.create_scatter_plot(
            df=gerencia_df,
            x_col='qtd_infracoes',
            y_col='taxa_conversao',
            title="Relação entre Volume e Performance",
            size_col='valor_total',
            trendline=True
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        # Tabela
        st.markdown("### 📋 Dados Completos")

        st.dataframe(
            gerencia_df.style.format({
                'qtd_infracoes': '{:,.0f}',
                'qtd_nfs': '{:,.0f}',
                'taxa_conversao': '{:.2f}%',
                'valor_total': 'R$ {:,.2f}',
                'media_dias_notificacao': '{:.1f}'
            }).background_gradient(subset=['taxa_conversao'], cmap='RdYlGn'),
            use_container_width=True,
            height=400
        )

        create_download_button(gerencia_df, f"performance_gerencias_{datetime.now().strftime('%Y%m%d')}.csv")

    # ===== TAB AFRES =====
    with tab2:
        st.markdown("## Análise por AFRE")

        afre_df = load_afre_performance(db)

        if afre_df is None or afre_df.empty:
            st.warning("⚠️ Dados não disponíveis")
            return

        # KPIs
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total AFREs", len(afre_df))

        with col2:
            prod_media = afre_df['nfs_por_mes'].mean()
            st.metric("Produtividade Média", f"{prod_media:.2f} NFs/mês")

        with col3:
            melhor_afre = afre_df.nlargest(1, 'nfs_por_mes').iloc[0]
            st.metric("Melhor Produtividade", f"{melhor_afre['nfs_por_mes']:.2f}",
                     delta=melhor_afre.get('afre_nome', 'N/A'))

        with col4:
            total_nfs = afre_df['qtd_nfs'].sum()
            st.metric("Total NFs", f"{total_nfs:,.0f}")

        st.markdown("---")

        # Faixas de produtividade
        afre_df['faixa'] = pd.cut(
            afre_df['nfs_por_mes'],
            bins=[0, 2, 4, 6, 8, 1000],
            labels=['Muito Baixa', 'Baixa', 'Média', 'Alta', 'Muito Alta']
        )

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 📊 Distribuição por Faixa de Produtividade")

            faixa_counts = afre_df['faixa'].value_counts().reset_index()
            faixa_counts.columns = ['Faixa', 'Quantidade']

            fig_faixa = chart_builder.create_pie_chart(
                df=faixa_counts,
                names_col='Faixa',
                values_col='Quantidade',
                title="",
                hole=0.4
            )
            st.plotly_chart(fig_faixa, use_container_width=True)

        with col2:
            st.markdown("### 🏆 Top 10 AFREs Mais Produtivos")

            top_prod = afre_df.nlargest(10, 'nfs_por_mes')

            fig_top = chart_builder.create_horizontal_bar_ranking(
                df=top_prod,
                category_col='afre_nome',
                value_col='nfs_por_mes',
                title="",
                top_n=10,
                color_scale=True
            )
            st.plotly_chart(fig_top, use_container_width=True)

        # Alertas
        st.markdown("### 🚨 Análise de Performance")

        afres_abaixo_meta = afre_df[afre_df['nfs_por_mes'] < META_PRODUTIVIDADE_AFRE]

        if len(afres_abaixo_meta) > 0:
            st.warning(f"""
            **⚠️ AFREs Abaixo da Meta de Produtividade:**
            - Total: {len(afres_abaixo_meta)} AFREs ({len(afres_abaixo_meta)/len(afre_df)*100:.1f}%)
            - Meta: {META_PRODUTIVIDADE_AFRE} NFs/mês
            - Necessitam acompanhamento e suporte
            """)
        else:
            st.success("✅ Todos os AFREs estão acima da meta de produtividade!")

        # Tabela
        st.markdown("### 📋 Ranking Completo")

        st.dataframe(
            afre_df[['afre_nome', 'qtd_nfs', 'taxa_conversao', 'valor_total', 'nfs_por_mes', 'faixa']].style.format({
                'qtd_nfs': '{:,.0f}',
                'taxa_conversao': '{:.2f}%',
                'valor_total': 'R$ {:,.2f}',
                'nfs_por_mes': '{:.2f}'
            }).background_gradient(subset=['nfs_por_mes'], cmap='RdYlGn'),
            use_container_width=True,
            height=400
        )

        create_download_button(afre_df, f"performance_afres_{datetime.now().strftime('%Y%m%d')}.csv")
