"""
Módulo de Visualizações Avançadas
Biblioteca completa de gráficos e dashboards visuais
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any
import streamlit as st

from src.config.settings import THEME_COLORS, COLOR_SCALES, META_CONVERSAO, META_DIAS_NOTIFICACAO


class ChartBuilder:
    """Construtor avançado de gráficos"""

    def __init__(self, theme: str = 'plotly'):
        self.theme = theme
        self.colors = THEME_COLORS

    # ===== GRÁFICOS DE MÉTRICAS E KPIS =====

    @staticmethod
    def create_metric_card(
        title: str,
        value: Any,
        delta: Optional[str] = None,
        suffix: str = "",
        prefix: str = "",
        color: str = "primary"
    ) -> str:
        """Cria card HTML de métrica estilizado"""
        color_class = f"metric-card-{color}" if color in ['success', 'warning', 'info'] else "metric-card"

        delta_html = ""
        if delta:
            delta_html = f'<div style="font-size: 0.9rem; margin-top: 0.5rem;">{delta}</div>'

        return f"""
        <div class="{color_class}">
            <div style="font-size: 0.9rem; opacity: 0.9;">{title}</div>
            <div style="font-size: 2rem; font-weight: bold; margin-top: 0.5rem;">
                {prefix}{value}{suffix}
            </div>
            {delta_html}
        </div>
        """

    @staticmethod
    def create_kpi_gauge(
        value: float,
        title: str,
        max_value: float = 100,
        threshold: float = None,
        suffix: str = "%"
    ) -> go.Figure:
        """Cria gauge de KPI"""
        color = THEME_COLORS['success'] if threshold and value >= threshold else THEME_COLORS['warning']

        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=value,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': title, 'font': {'size': 20}},
            number={'suffix': suffix},
            gauge={
                'axis': {'range': [None, max_value]},
                'bar': {'color': color},
                'bgcolor': "white",
                'borderwidth': 2,
                'bordercolor': "gray",
                'steps': [
                    {'range': [0, max_value * 0.5], 'color': '#ffebee'},
                    {'range': [max_value * 0.5, max_value * 0.75], 'color': '#fff9c4'},
                    {'range': [max_value * 0.75, max_value], 'color': '#e8f5e9'}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': threshold if threshold else max_value * 0.7
                }
            }
        ))

        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=60, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )

        return fig

    # ===== GRÁFICOS TEMPORAIS =====

    @staticmethod
    def create_time_series(
        df: pd.DataFrame,
        x_col: str,
        y_cols: List[str],
        title: str,
        labels: Dict[str, str] = None,
        show_trend: bool = True
    ) -> go.Figure:
        """Cria gráfico de série temporal com múltiplas linhas"""
        fig = go.Figure()

        colors = px.colors.qualitative.Set2

        for idx, col in enumerate(y_cols):
            fig.add_trace(go.Scatter(
                x=df[x_col],
                y=df[col],
                name=labels.get(col, col) if labels else col,
                mode='lines+markers',
                line=dict(width=3, color=colors[idx % len(colors)]),
                marker=dict(size=8)
            ))

            # Adiciona linha de tendência se solicitado
            if show_trend and len(df) > 1:
                z = np.polyfit(range(len(df)), df[col], 1)
                p = np.poly1d(z)
                fig.add_trace(go.Scatter(
                    x=df[x_col],
                    y=p(range(len(df))),
                    name=f'Tendência {labels.get(col, col) if labels else col}',
                    mode='lines',
                    line=dict(dash='dash', width=2, color=colors[idx % len(colors)]),
                    opacity=0.5
                ))

        fig.update_layout(
            title=title,
            xaxis_title=x_col,
            yaxis_title="Valor",
            hovermode='x unified',
            height=500,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        return fig

    @staticmethod
    def create_area_chart(
        df: pd.DataFrame,
        x_col: str,
        y_cols: List[str],
        title: str,
        stacked: bool = True
    ) -> go.Figure:
        """Cria gráfico de área"""
        fig = go.Figure()

        stackgroup = 'one' if stacked else None

        for col in y_cols:
            fig.add_trace(go.Scatter(
                x=df[x_col],
                y=df[col],
                name=col,
                mode='lines',
                stackgroup=stackgroup,
                fillcolor='rgba(0,0,0,0.1)',
                line=dict(width=2)
            ))

        fig.update_layout(
            title=title,
            xaxis_title=x_col,
            yaxis_title="Valor",
            hovermode='x unified',
            height=500
        )

        return fig

    # ===== GRÁFICOS DE COMPARAÇÃO =====

    @staticmethod
    def create_bar_chart(
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        orientation: str = 'v',
        color_col: Optional[str] = None,
        top_n: int = None
    ) -> go.Figure:
        """Cria gráfico de barras"""
        plot_df = df.copy()

        if top_n:
            plot_df = plot_df.nlargest(top_n, y_col)

        if color_col:
            fig = px.bar(
                plot_df,
                x=x_col if orientation == 'v' else y_col,
                y=y_col if orientation == 'v' else x_col,
                color=color_col,
                title=title,
                orientation=orientation,
                color_continuous_scale=COLOR_SCALES['sequential_blue']
            )
        else:
            fig = px.bar(
                plot_df,
                x=x_col if orientation == 'v' else y_col,
                y=y_col if orientation == 'v' else x_col,
                title=title,
                orientation=orientation,
                color_discrete_sequence=[THEME_COLORS['primary']]
            )

        fig.update_layout(height=500, showlegend=True)

        return fig

    @staticmethod
    def create_grouped_bar_chart(
        df: pd.DataFrame,
        x_col: str,
        y_cols: List[str],
        title: str,
        barmode: str = 'group'
    ) -> go.Figure:
        """Cria gráfico de barras agrupadas"""
        fig = go.Figure()

        colors = px.colors.qualitative.Set2

        for idx, col in enumerate(y_cols):
            fig.add_trace(go.Bar(
                name=col,
                x=df[x_col],
                y=df[col],
                marker_color=colors[idx % len(colors)]
            ))

        fig.update_layout(
            title=title,
            barmode=barmode,
            height=500,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        return fig

    # ===== GRÁFICOS DE DISTRIBUIÇÃO =====

    @staticmethod
    def create_pie_chart(
        df: pd.DataFrame,
        names_col: str,
        values_col: str,
        title: str,
        hole: float = 0.4
    ) -> go.Figure:
        """Cria gráfico de pizza/donut"""
        fig = px.pie(
            df,
            names=names_col,
            values=values_col,
            title=title,
            hole=hole,
            color_discrete_sequence=px.colors.qualitative.Set3
        )

        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Valor: %{value}<br>Percentual: %{percent}<extra></extra>'
        )

        fig.update_layout(height=500, showlegend=True)

        return fig

    @staticmethod
    def create_sunburst(
        df: pd.DataFrame,
        path: List[str],
        values_col: str,
        title: str
    ) -> go.Figure:
        """Cria gráfico sunburst (hierárquico)"""
        fig = px.sunburst(
            df,
            path=path,
            values=values_col,
            title=title,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )

        fig.update_layout(height=600)

        return fig

    @staticmethod
    def create_treemap(
        df: pd.DataFrame,
        path: List[str],
        values_col: str,
        title: str,
        color_col: Optional[str] = None
    ) -> go.Figure:
        """Cria treemap"""
        fig = px.treemap(
            df,
            path=path,
            values=values_col,
            title=title,
            color=color_col if color_col else values_col,
            color_continuous_scale=COLOR_SCALES['heatmap']
        )

        fig.update_layout(height=600)

        return fig

    # ===== GRÁFICOS DE CORRELAÇÃO =====

    @staticmethod
    def create_scatter_plot(
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        size_col: Optional[str] = None,
        color_col: Optional[str] = None,
        trendline: bool = True
    ) -> go.Figure:
        """Cria gráfico de dispersão"""
        fig = px.scatter(
            df,
            x=x_col,
            y=y_col,
            size=size_col,
            color=color_col,
            title=title,
            trendline='ols' if trendline else None,
            hover_data=df.columns,
            color_continuous_scale=COLOR_SCALES['sequential_blue']
        )

        fig.update_layout(height=500)

        return fig

    @staticmethod
    def create_heatmap(
        df: pd.DataFrame,
        title: str,
        color_scale: str = 'Viridis'
    ) -> go.Figure:
        """Cria heatmap de correlação"""
        # Calcula matriz de correlação
        corr_matrix = df.select_dtypes(include=[np.number]).corr()

        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns,
            y=corr_matrix.columns,
            colorscale=color_scale,
            zmid=0,
            text=corr_matrix.values.round(2),
            texttemplate='%{text}',
            textfont={"size": 10},
            colorbar=dict(title="Correlação")
        ))

        fig.update_layout(
            title=title,
            height=600,
            xaxis={'side': 'bottom'},
            yaxis={'side': 'left'}
        )

        return fig

    # ===== GRÁFICOS DE BOX E VIOLIN =====

    @staticmethod
    def create_box_plot(
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: str
    ) -> go.Figure:
        """Cria box plot"""
        fig = px.box(
            df,
            x=x_col,
            y=y_col,
            title=title,
            color=x_col,
            color_discrete_sequence=px.colors.qualitative.Set2
        )

        fig.update_layout(height=500, showlegend=False)

        return fig

    @staticmethod
    def create_violin_plot(
        df: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: str
    ) -> go.Figure:
        """Cria violin plot"""
        fig = px.violin(
            df,
            x=x_col,
            y=y_col,
            title=title,
            color=x_col,
            box=True,
            points="all",
            color_discrete_sequence=px.colors.qualitative.Set2
        )

        fig.update_layout(height=500, showlegend=False)

        return fig

    # ===== GRÁFICOS DE RANKING =====

    @staticmethod
    def create_horizontal_bar_ranking(
        df: pd.DataFrame,
        category_col: str,
        value_col: str,
        title: str,
        top_n: int = 20,
        color_scale: bool = True
    ) -> go.Figure:
        """Cria ranking horizontal com barras"""
        plot_df = df.nlargest(top_n, value_col).sort_values(value_col)

        if color_scale:
            colors = plot_df[value_col]
            colorscale = COLOR_SCALES['sequential_blue']
        else:
            colors = THEME_COLORS['primary']
            colorscale = None

        fig = go.Figure(go.Bar(
            x=plot_df[value_col],
            y=plot_df[category_col],
            orientation='h',
            marker=dict(
                color=colors,
                colorscale=colorscale,
                showscale=color_scale
            ),
            text=plot_df[value_col],
            texttemplate='%{text:.2s}',
            textposition='outside'
        ))

        fig.update_layout(
            title=title,
            xaxis_title=value_col,
            yaxis_title=category_col,
            height=max(400, top_n * 25),
            showlegend=False
        )

        return fig

    # ===== GRÁFICOS COMBINADOS =====

    @staticmethod
    def create_combo_chart(
        df: pd.DataFrame,
        x_col: str,
        bar_cols: List[str],
        line_cols: List[str],
        title: str
    ) -> go.Figure:
        """Cria gráfico combinado de barras e linhas"""
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        colors_bar = px.colors.qualitative.Set1
        colors_line = px.colors.qualitative.Set2

        # Adiciona barras
        for idx, col in enumerate(bar_cols):
            fig.add_trace(
                go.Bar(
                    name=col,
                    x=df[x_col],
                    y=df[col],
                    marker_color=colors_bar[idx % len(colors_bar)]
                ),
                secondary_y=False
            )

        # Adiciona linhas
        for idx, col in enumerate(line_cols):
            fig.add_trace(
                go.Scatter(
                    name=col,
                    x=df[x_col],
                    y=df[col],
                    mode='lines+markers',
                    line=dict(width=3, color=colors_line[idx % len(colors_line)]),
                    marker=dict(size=8)
                ),
                secondary_y=True
            )

        fig.update_layout(
            title=title,
            hovermode='x unified',
            height=500,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )

        fig.update_yaxes(title_text="Valores (Barras)", secondary_y=False)
        fig.update_yaxes(title_text="Valores (Linhas)", secondary_y=True)

        return fig

    # ===== GRÁFICOS GEOGRÁFICOS =====

    @staticmethod
    def create_choropleth_map(
        df: pd.DataFrame,
        locations_col: str,
        values_col: str,
        title: str,
        locationmode: str = 'USA-states'
    ) -> go.Figure:
        """Cria mapa coroplético"""
        fig = px.choropleth(
            df,
            locations=locations_col,
            locationmode=locationmode,
            color=values_col,
            hover_name=locations_col,
            color_continuous_scale=COLOR_SCALES['heatmap'],
            title=title
        )

        fig.update_layout(height=600)

        return fig

    # ===== GRÁFICOS DE FUNNEL =====

    @staticmethod
    def create_funnel_chart(
        df: pd.DataFrame,
        stage_col: str,
        value_col: str,
        title: str
    ) -> go.Figure:
        """Cria gráfico de funil"""
        fig = go.Figure(go.Funnel(
            y=df[stage_col],
            x=df[value_col],
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(
                color=px.colors.sequential.Blues,
                line=dict(width=2, color="white")
            )
        ))

        fig.update_layout(
            title=title,
            height=500
        )

        return fig

    # ===== DASHBOARD COMPLETO =====

    @staticmethod
    def create_dashboard_grid(
        metrics: List[Dict[str, Any]],
        charts: List[go.Figure],
        layout: str = '2x2'
    ):
        """Cria grid de dashboard com métricas e gráficos"""
        # Esta função retorna elementos para renderizar no Streamlit
        # Não retorna uma figura, mas organiza o layout

        # Renderiza métricas
        if metrics:
            cols = st.columns(len(metrics))
            for idx, metric in enumerate(metrics):
                with cols[idx]:
                    st.markdown(
                        ChartBuilder.create_metric_card(**metric),
                        unsafe_allow_html=True
                    )

        # Renderiza gráficos conforme layout
        if charts:
            if layout == '2x2' and len(charts) >= 4:
                col1, col2 = st.columns(2)
                with col1:
                    st.plotly_chart(charts[0], use_container_width=True)
                    st.plotly_chart(charts[2], use_container_width=True)
                with col2:
                    st.plotly_chart(charts[1], use_container_width=True)
                    st.plotly_chart(charts[3], use_container_width=True)
            elif layout == '1x2':
                for i in range(0, len(charts), 2):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.plotly_chart(charts[i], use_container_width=True)
                    if i + 1 < len(charts):
                        with col2:
                            st.plotly_chart(charts[i + 1], use_container_width=True)
            else:
                for chart in charts:
                    st.plotly_chart(chart, use_container_width=True)


# Funções auxiliares para formatação
def format_currency(value: float) -> str:
    """Formata valor como moeda brasileira"""
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_percentage(value: float) -> str:
    """Formata valor como percentual"""
    return f"{value:.2f}%"


def format_number(value: float) -> str:
    """Formata número com separadores"""
    return f"{value:,.0f}".replace(",", ".")
