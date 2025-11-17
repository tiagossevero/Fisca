"""
Dashboard Executivo
Painel principal com KPIs, métricas e visão geral do sistema
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

from src.modules.database import get_database_connection
from src.modules.cache_manager import (
    load_dashboard_metrics,
    load_temporal_evolution,
    load_performance_by_gerencia,
    load_afre_performance
)
from src.modules.charts import ChartBuilder, format_currency, format_percentage, format_number
from src.utils.helpers import Calculator, AlertGenerator, Formatter, create_download_button
from src.config.settings import META_CONVERSAO, META_DIAS_NOTIFICACAO, META_PRODUTIVIDADE_AFRE


def render():
    """Renderiza Dashboard Executivo"""
    st.title("📊 Dashboard Executivo")
    st.markdown("### Visão Geral do Sistema de Fiscalizações")

    # Obter conexão com banco
    db = get_database_connection()

    # Filtros
    with st.sidebar:
        st.header("⚙️ Filtros")

        ano_inicio = st.selectbox(
            "Ano Início",
            options=list(range(2020, 2026)),
            index=0
        )

        ano_fim = st.selectbox(
            "Ano Fim",
            options=list(range(2020, 2026)),
            index=5
        )

        periodo_temporal = st.selectbox(
            "Agrupamento Temporal",
            options=['Diário', 'Mensal', 'Anual'],
            index=1
        )

        st.markdown("---")
        if st.button("🔄 Atualizar Dados"):
            st.cache_data.clear()
            st.rerun()

    # Carregar dados
    with st.spinner("Carregando métricas..."):
        metrics_df = load_dashboard_metrics(db, ano_inicio, ano_fim)

        if metrics_df is None or metrics_df.empty:
            st.warning("⚠️ Nenhum dado encontrado para o período selecionado")
            return

        # Extrair métricas
        metrics = metrics_df.iloc[0].to_dict()

    # ===== SEÇÃO 1: KPIs PRINCIPAIS =====
    st.markdown("## 📈 Indicadores Principais")

    chart_builder = ChartBuilder()
    calculator = Calculator()

    # Calcular KPIs derivados
    taxa_conversao = calculator.calculate_conversion_rate(
        metrics.get('total_nfs', 0),
        metrics.get('total_infracoes', 0)
    )

    valor_medio_infracao = calculator.calculate_average_ticket(
        metrics.get('valor_total_infracoes', 0),
        metrics.get('total_infracoes', 1)
    )

    valor_medio_nf = calculator.calculate_average_ticket(
        metrics.get('valor_total_nfs', 0),
        metrics.get('total_nfs', 1)
    )

    # Cards de Métricas
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(
            chart_builder.create_metric_card(
                title="Total de Infrações",
                value=format_number(metrics.get('total_infracoes', 0)),
                color="info"
            ),
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            chart_builder.create_metric_card(
                title="Notificações Emitidas",
                value=format_number(metrics.get('total_nfs', 0)),
                delta=f"Taxa: {format_percentage(taxa_conversao)}",
                color="success" if taxa_conversao >= META_CONVERSAO else "warning"
            ),
            unsafe_allow_html=True
        )

    with col3:
        st.markdown(
            chart_builder.create_metric_card(
                title="Valor Total Autuado",
                value=Formatter.abbreviate_number(metrics.get('valor_total_infracoes', 0)),
                prefix="R$ ",
                color="primary"
            ),
            unsafe_allow_html=True
        )

    with col4:
        st.markdown(
            chart_builder.create_metric_card(
                title="Empresas Fiscalizadas",
                value=format_number(metrics.get('total_empresas', 0)),
                color="info"
            ),
            unsafe_allow_html=True
        )

    st.markdown("---")

    # Segunda linha de KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        media_dias = metrics.get('media_dias_notificacao', 0) or 0
        st.markdown(
            chart_builder.create_metric_card(
                title="Tempo Médio (Infração → NF)",
                value=f"{media_dias:.1f}",
                suffix=" dias",
                color="success" if media_dias <= META_DIAS_NOTIFICACAO else "warning"
            ),
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            chart_builder.create_metric_card(
                title="Valor Médio por Infração",
                value=Formatter.abbreviate_number(valor_medio_infracao),
                prefix="R$ ",
                color="info"
            ),
            unsafe_allow_html=True
        )

    with col3:
        st.markdown(
            chart_builder.create_metric_card(
                title="AFREs Ativos",
                value=format_number(metrics.get('total_afres_ativos', 0)),
                color="primary"
            ),
            unsafe_allow_html=True
        )

    with col4:
        produtividade_media = calculator.calculate_average_ticket(
            metrics.get('total_nfs', 0),
            metrics.get('total_afres_ativos', 1)
        )
        st.markdown(
            chart_builder.create_metric_card(
                title="NFs por AFRE",
                value=f"{produtividade_media:.1f}",
                color="success"
            ),
            unsafe_allow_html=True
        )

    st.markdown("---")

    # ===== SEÇÃO 2: GAUGES DE PERFORMANCE =====
    st.markdown("## 🎯 Metas e Performance")

    col1, col2, col3 = st.columns(3)

    with col1:
        fig_gauge_conversao = chart_builder.create_kpi_gauge(
            value=taxa_conversao,
            title="Taxa de Conversão",
            max_value=100,
            threshold=META_CONVERSAO
        )
        st.plotly_chart(fig_gauge_conversao, use_container_width=True)

    with col2:
        # Normalizar dias para percentual (inverso - menos dias é melhor)
        dias_performance = max(0, 100 - (media_dias / META_DIAS_NOTIFICACAO * 100))
        fig_gauge_tempo = chart_builder.create_kpi_gauge(
            value=dias_performance,
            title="Performance de Tempo",
            max_value=100,
            threshold=70,
            suffix="%"
        )
        st.plotly_chart(fig_gauge_tempo, use_container_width=True)

    with col3:
        # Carregar performance de AFREs para calcular produtividade média
        afre_df = load_afre_performance(db)
        if afre_df is not None and not afre_df.empty:
            prod_media = afre_df['nfs_por_mes'].mean()
            prod_performance = min(100, (prod_media / META_PRODUTIVIDADE_AFRE) * 100)
        else:
            prod_performance = 0

        fig_gauge_prod = chart_builder.create_kpi_gauge(
            value=prod_performance,
            title="Performance Produtividade",
            max_value=100,
            threshold=80,
            suffix="%"
        )
        st.plotly_chart(fig_gauge_prod, use_container_width=True)

    st.markdown("---")

    # ===== SEÇÃO 3: ALERTAS E RECOMENDAÇÕES =====
    st.markdown("## 🚨 Alertas e Recomendações")

    alert_gen = AlertGenerator()

    # Gerar alertas
    alert_conversao = alert_gen.check_conversion_rate(taxa_conversao, META_CONVERSAO)
    alert_tempo = alert_gen.check_processing_time(media_dias, META_DIAS_NOTIFICACAO)

    col1, col2 = st.columns(2)

    with col1:
        if alert_conversao['type'] == 'warning':
            st.warning(f"**{alert_conversao['title']}**\n\n{alert_conversao['message']}")
        else:
            st.success(f"**{alert_conversao['title']}**\n\n{alert_conversao['message']}")

    with col2:
        if alert_tempo['type'] == 'warning':
            st.warning(f"**{alert_tempo['title']}**\n\n{alert_tempo['message']}")
        else:
            st.success(f"**{alert_tempo['title']}**\n\n{alert_tempo['message']}")

    st.markdown("---")

    # ===== SEÇÃO 4: EVOLUÇÃO TEMPORAL =====
    st.markdown("## 📊 Evolução Temporal")

    # Mapear período
    period_map = {'Diário': 'dia', 'Mensal': 'mes', 'Anual': 'ano'}
    period = period_map.get(periodo_temporal, 'mes')

    temporal_df = load_temporal_evolution(db, period)

    if temporal_df is not None and not temporal_df.empty:
        # Calcular taxa de conversão por período
        temporal_df['taxa_conversao'] = calculator.calculate_conversion_rate(
            temporal_df['qtd_nfs'],
            temporal_df['qtd_infracoes']
        )

        # Gráfico de evolução de volume
        fig_volume = chart_builder.create_time_series(
            df=temporal_df,
            x_col='periodo',
            y_cols=['qtd_infracoes', 'qtd_nfs'],
            title=f"Evolução de Infrações e Notificações ({periodo_temporal})",
            labels={
                'qtd_infracoes': 'Infrações',
                'qtd_nfs': 'Notificações'
            },
            show_trend=True
        )
        st.plotly_chart(fig_volume, use_container_width=True)

        # Gráfico de evolução de valores
        fig_valores = chart_builder.create_area_chart(
            df=temporal_df,
            x_col='periodo',
            y_cols=['valor_total'],
            title=f"Evolução de Valores ({periodo_temporal})",
            stacked=False
        )
        st.plotly_chart(fig_valores, use_container_width=True)

        # Gráfico combinado: Volume (barras) + Taxa de Conversão (linha)
        fig_combo = chart_builder.create_combo_chart(
            df=temporal_df,
            x_col='periodo',
            bar_cols=['qtd_infracoes'],
            line_cols=['taxa_conversao'],
            title=f"Volume de Infrações vs Taxa de Conversão ({periodo_temporal})"
        )
        st.plotly_chart(fig_combo, use_container_width=True)

    st.markdown("---")

    # ===== SEÇÃO 5: TOP PERFORMERS =====
    st.markdown("## 🏆 Top Performers")

    col1, col2 = st.columns(2)

    # Top Gerências
    gerencia_df = load_performance_by_gerencia(db)

    if gerencia_df is not None and not gerencia_df.empty:
        with col1:
            st.markdown("### 🏢 Top 10 Gerências por Valor")

            top_gerencias = gerencia_df.nlargest(10, 'valor_total')

            fig_gerencias = chart_builder.create_horizontal_bar_ranking(
                df=top_gerencias,
                category_col='gerencia',
                value_col='valor_total',
                title="",
                top_n=10,
                color_scale=True
            )
            st.plotly_chart(fig_gerencias, use_container_width=True)

    # Top AFREs
    if afre_df is not None and not afre_df.empty:
        with col2:
            st.markdown("### 👤 Top 10 AFREs por NFs Emitidas")

            top_afres = afre_df.nlargest(10, 'qtd_nfs')

            fig_afres = chart_builder.create_horizontal_bar_ranking(
                df=top_afres,
                category_col='afre_nome',
                value_col='qtd_nfs',
                title="",
                top_n=10,
                color_scale=True
            )
            st.plotly_chart(fig_afres, use_container_width=True)

    st.markdown("---")

    # ===== SEÇÃO 6: DISTRIBUIÇÃO E CONCENTRAÇÃO =====
    st.markdown("## 📊 Análise de Distribuição")

    col1, col2 = st.columns(2)

    with col1:
        if gerencia_df is not None and not gerencia_df.empty:
            # Calcular concentração
            calc = Calculator()
            concentracao = calc.calculate_concentration_index(gerencia_df, 'valor_total', top_n=5)

            st.metric(
                label="Concentração Top 5 Gerências",
                value=f"{concentracao:.1f}%",
                delta="do valor total"
            )

            # Pizza das top 10
            top_10_gerencias = gerencia_df.nlargest(10, 'valor_total')
            fig_pizza = chart_builder.create_pie_chart(
                df=top_10_gerencias,
                names_col='gerencia',
                values_col='valor_total',
                title="Distribuição de Valor - Top 10 Gerências",
                hole=0.4
            )
            st.plotly_chart(fig_pizza, use_container_width=True)

    with col2:
        if afre_df is not None and not afre_df.empty:
            # Distribuição de produtividade
            st.markdown("### Distribuição de Produtividade (NFs/mês)")

            fig_box = chart_builder.create_box_plot(
                df=afre_df.head(50),
                x_col=None,
                y_col='nfs_por_mes',
                title="Distribuição de Produtividade dos AFREs"
            )
            # Ajustar para mostrar apenas a distribuição
            fig_box.update_xaxes(visible=False)
            st.plotly_chart(fig_box, use_container_width=True)

            # Estatísticas
            st.markdown("**Estatísticas de Produtividade:**")
            st.write(f"- Média: {afre_df['nfs_por_mes'].mean():.2f} NFs/mês")
            st.write(f"- Mediana: {afre_df['nfs_por_mes'].median():.2f} NFs/mês")
            st.write(f"- Máximo: {afre_df['nfs_por_mes'].max():.2f} NFs/mês")
            st.write(f"- Mínimo: {afre_df['nfs_por_mes'].min():.2f} NFs/mês")

    st.markdown("---")

    # ===== SEÇÃO 7: EXPORTAÇÃO DE DADOS =====
    st.markdown("## 📥 Exportar Dados")

    col1, col2, col3 = st.columns(3)

    with col1:
        if temporal_df is not None and not temporal_df.empty:
            create_download_button(
                temporal_df,
                f"evolucao_temporal_{datetime.now().strftime('%Y%m%d')}.csv",
                "📊 Exportar Evolução Temporal"
            )

    with col2:
        if gerencia_df is not None and not gerencia_df.empty:
            create_download_button(
                gerencia_df,
                f"performance_gerencias_{datetime.now().strftime('%Y%m%d')}.csv",
                "🏢 Exportar Performance Gerências"
            )

    with col3:
        if afre_df is not None and not afre_df.empty:
            create_download_button(
                afre_df,
                f"performance_afres_{datetime.now().strftime('%Y%m%d')}.csv",
                "👤 Exportar Performance AFREs"
            )

    # Resumo final
    st.markdown("---")
    st.markdown("### 📌 Resumo do Período")

    summary_col1, summary_col2, summary_col3 = st.columns(3)

    with summary_col1:
        st.info(f"""
        **Período Analisado:**
        - De: {ano_inicio}
        - Até: {ano_fim}
        - Total de registros: {format_number(metrics.get('total_infracoes', 0))}
        """)

    with summary_col2:
        st.success(f"""
        **Performance Geral:**
        - Taxa de Conversão: {format_percentage(taxa_conversao)}
        - Meta: {format_percentage(META_CONVERSAO)}
        - Status: {'✅ Atingida' if taxa_conversao >= META_CONVERSAO else '⚠️ Abaixo da Meta'}
        """)

    with summary_col3:
        st.warning(f"""
        **Oportunidades:**
        - Tempo médio: {media_dias:.1f} dias
        - Meta: {META_DIAS_NOTIFICACAO} dias
        - Redução necessária: {max(0, media_dias - META_DIAS_NOTIFICACAO):.1f} dias
        """)
